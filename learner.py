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
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "600"))  # every 10 min

# ========================================
# DB Setup
# ========================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS tickets (
        ticket_id TEXT PRIMARY KEY,
        subject TEXT,
        content TEXT,
        created_at TEXT,
        last_seen TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS companies (
        company_id TEXT PRIMARY KEY,
        name TEXT,
        domain TEXT,
        last_seen TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS contacts (
        contact_id TEXT PRIMARY KEY,
        email TEXT,
        firstname TEXT,
        lastname TEXT,
        company_id TEXT,
        last_seen TEXT
    )""")
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
app = FastAPI(title="HubSpot Learner v2")

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
    suggestions = []

    # Data cleanup: missing company domains
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM companies WHERE domain IS NULL OR domain=''")
    missing_domains = c.fetchone()[0]
    if missing_domains > 0:
        suggestions.append(f"{missing_domains} companies are missing domain information.")

    # Data cleanup: contacts without email
    c.execute("SELECT COUNT(*) FROM contacts WHERE email IS NULL OR email=''")
    missing_emails = c.fetchone()[0]
    if missing_emails > 0:
        suggestions.append(f"{missing_emails} contacts are missing email addresses.")

    conn.close()

    # KB article ideas (most common ticket subjects)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""SELECT subject, COUNT(*) as freq 
                 FROM tickets 
                 WHERE subject IS NOT NULL 
                 GROUP BY subject 
                 ORDER BY freq DESC 
                 LIMIT 5""")
    kb_candidates = [{"subject": r[0], "count": r[1]} for r in c.fetchall()]
    conn.close()

    return {
        "suggestions": suggestions,
        "kb_candidates": kb_candidates
    }

# ========================================
# Background Loop
# ========================================
async def learning_loop():
    while True:
        try:
            print("[Learner] Polling HubSpot data...")

            async with httpx.AsyncClient(headers={"Authorization": BRIDGE_SECRET}) as client:

                # Tickets
                r = await client.post(f"{CONNECTOR_URL}/tickets/search",
                                      json={"limit": 20, "properties": ["subject", "content"]},
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

                # Companies
                r = await client.get(f"{CONNECTOR_URL}/companies/get/1")  # Example test
                # TODO: add proper fetch-all logic via connector (batch support)
                # For now, learner v2 focuses on tickets but schema is ready.

                # TODO: Same approach for contacts & deals once connector exposes batch endpoints.

            print("[Learner] Polling complete.")

        except Exception as e:
            print(f"[Learner] Exception: {e}")

        await asyncio.sleep(POLL_INTERVAL)
