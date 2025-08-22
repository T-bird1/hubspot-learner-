import asyncio
import httpx
import os
import sqlite3
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from datetime import datetime

# ========================================
# Config
# ========================================
CONNECTOR_URL = os.getenv("CONNECTOR_URL", "https://hubspot-connector.onrender.com")
BRIDGE_SECRET = os.getenv("BRIDGE_SECRET", "")
DB_PATH = os.getenv("DB_PATH", "learner.db")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "900"))  # every 15 minutes

# ========================================
# DB Setup
# ========================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Tickets
    c.execute("""CREATE TABLE IF NOT EXISTS tickets (
        ticket_id TEXT PRIMARY KEY,
        subject TEXT,
        content TEXT,
        created_at TEXT,
        last_seen TEXT
    )""")
    # Companies
    c.execute("""CREATE TABLE IF NOT EXISTS companies (
        company_id TEXT PRIMARY KEY,
        name TEXT,
        domain TEXT,
        last_seen TEXT
    )""")
    # Contacts
    c.execute("""CREATE TABLE IF NOT EXISTS contacts (
        contact_id TEXT PRIMARY KEY,
        email TEXT,
        firstname TEXT,
        lastname TEXT,
        company_id TEXT,
        last_seen TEXT
    )""")
    # Deals
    c.execute("""CREATE TABLE IF NOT EXISTS deals (
        deal_id TEXT PRIMARY KEY,
        dealname TEXT,
        amount REAL,
        stage TEXT,
        company_id TEXT,
        last_seen TEXT
    )""")
    conn.commit()
    conn.close()

def upsert(table, data: dict, pk: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    cols = ", ".join(data.keys())
    placeholders = ", ".join("?" * len(data))
    update = ", ".join([f"{k}=excluded.{k}" for k in data.keys()])
    query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT({pk}) DO UPDATE SET {update}"
    c.execute(query, list(data.values()))
    conn.commit()
    conn.close()

def count(table):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(f"SELECT COUNT(*) FROM {table}")
    result = c.fetchone()[0]
    conn.close()
    return result

# ========================================
# FastAPI
# ========================================
app = FastAPI(title="HubSpot Learner (Extended)")

@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(learning_loop())

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/learning/status")
def learning_status():
    return {
        "tickets_indexed": count("tickets"),
        "companies_indexed": count("companies"),
        "contacts_indexed": count("contacts"),
        "deals_indexed": count("deals")
    }

@app.get("/learning/suggestions")
def learning_suggestions():
    cleanup = []
    kb_candidates = []
    workflow_candidates = []

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # -------------------------
    # Data Cleanup
    # -------------------------
    c.execute("SELECT COUNT(*) FROM companies WHERE domain IS NULL OR domain=''")
    if (missing := c.fetchone()[0]) > 0:
        cleanup.append(f"{missing} companies are missing domain information.")

    c.execute("SELECT COUNT(*) FROM contacts WHERE email IS NULL OR email=''")
    if (missing := c.fetchone()[0]) > 0:
        cleanup.append(f"{missing} contacts are missing email addresses.")

    c.execute("SELECT COUNT(*) FROM deals WHERE stage IS NULL OR stage=''")
    if (missing := c.fetchone()[0]) > 0:
        cleanup.append(f"{missing} deals are missing pipeline stage.")

    # -------------------------
    # Knowledge Base Candidates
    # -------------------------
    c.execute("""SELECT subject, COUNT(*) as freq 
                 FROM tickets 
                 WHERE subject IS NOT NULL AND subject != '' 
                 GROUP BY subject 
                 ORDER BY freq DESC 
                 LIMIT 5""")
    kb_candidates = [{"subject": r[0], "count": r[1]} for r in c.fetchall()]

    # -------------------------
    # Workflow Candidates
    # -------------------------

    # Candidate 1: Alert if company has >5 open tickets
    c.execute("""SELECT company_id, COUNT(*) 
                 FROM deals 
                 WHERE company_id IS NOT NULL 
                 GROUP BY company_id 
                 HAVING COUNT(*) > 5""")
    heavy_companies = [row[0] for row in c.fetchall()]
    if heavy_companies:
        workflow_candidates.append(
            f"Create an alert workflow for CSMs when a company exceeds 5 active deals. {len(heavy_companies)} companies qualify."
        )

    # Candidate 2: Escalation for old tickets
    c.execute("""SELECT COUNT(*) 
                 FROM tickets 
                 WHERE created_at < date('now','-30 days')""")
    old_tickets = c.fetchone()[0]
    if old_tickets > 0:
        workflow_candidates.append(f"Escalation workflow: {old_tickets} tickets have been open for >30 days.")

    # Candidate 3: Notify when deal lacks associated company
    c.execute("""SELECT COUNT(*) 
                 FROM deals 
                 WHERE company_id IS NULL OR company_id=''""")
    orphan_deals = c.fetchone()[0]
    if orphan_deals > 0:
        workflow_candidates.append(f"Notification workflow: {orphan_deals} deals have no associated company.")

    conn.close()

    return {
        "data_cleanup": cleanup,
        "kb_candidates": kb_candidates,
        "workflow_candidates": workflow_candidates
    }

# ========================================
# Background Loop
# ========================================
async def learning_loop():
    while True:
        try:
            print("[Learner] Polling HubSpot data...")

            async with httpx.AsyncClient(headers={"Authorization": BRIDGE_SECRET}) as client:

                # -------- Tickets --------
                r = await client.post(f"{CONNECTOR_URL}/ticketsSearch",
                                      json={"limit": 100, "properties": ["subject", "content", "createdate"]},
                                      timeout=60)
                if r.status_code == 200:
                    for t in r.json().get("results", []):
                        props = t.get("properties", {})
                        upsert("tickets", {
                            "ticket_id": t.get("id"),
                            "subject": props.get("subject"),
                            "content": props.get("content"),
                            "created_at": props.get("createdate"),
                            "last_seen": datetime.utcnow().isoformat()
                        }, "ticket_id")

                # -------- Companies --------
                r = await client.post(f"{CONNECTOR_URL}/companiesSearch",
                                      json={"limit": 100, "properties": ["name", "domain"]},
                                      timeout=60)
                if r.status_code == 200:
                    for cdata in r.json().get("results", []):
                        props = cdata.get("properties", {})
                        upsert("companies", {
                            "company_id": cdata.get("id"),
                            "name": props.get("name"),
                            "domain": props.get("domain"),
                            "last_seen": datetime.utcnow().isoformat()
                        }, "company_id")

                # -------- Contacts --------
                r = await client.post(f"{CONNECTOR_URL}/contactsSearch",
                                      json={"limit": 100, "properties": ["email", "firstname", "lastname", "company"]},
                                      timeout=60)
                if r.status_code == 200:
                    for cdata in r.json().get("results", []):
                        props = cdata.get("properties", {})
                        upsert("contacts", {
                            "contact_id": cdata.get("id"),
                            "email": props.get("email"),
                            "firstname": props.get("firstname"),
                            "lastname": props.get("lastname"),
                            "company_id": props.get("company"),
                            "last_seen": datetime.utcnow().isoformat()
                        }, "contact_id")

                # -------- Deals --------
                r = await client.post(f"{CONNECTOR_URL}/dealsSearch",
                                      json={"limit": 100, "properties": ["dealname", "amount", "dealstage", "associatedcompanyid"]},
                                      timeout=60)
                if r.status_code == 200:
                    for d in r.json().get("results", []):
                        props = d.get("properties", {})
                        upsert("deals", {
                            "deal_id": d.get("id"),
                            "dealname": props.get("dealname"),
                            "amount": props.get("amount"),
                            "stage": props.get("dealstage"),
                            "company_id": props.get("associatedcompanyid"),
                            "last_seen": datetime.utcnow().isoformat()
                        }, "deal_id")

            print("[Learner] Polling complete.")

        except Exception as e:
            print(f"[Learner] Exception: {e}")

        await asyncio.sleep(POLL_INTERVAL)
