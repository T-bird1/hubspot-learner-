from fastapi import FastAPI
from fastapi.responses import JSONResponse
import httpx, os, sqlite3, asyncio
from datetime import datetime

BRIDGE_URL = os.getenv("BRIDGE_URL")
BRIDGE_SECRET = os.getenv("BRIDGE_SECRET")

app = FastAPI(title="HubSpot Learner")

DB_PATH = "learner.db"

# ------------------------
# DB Setup
# ------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS tickets (id TEXT PRIMARY KEY, subject TEXT, createdate TEXT, company_id TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS contacts (id TEXT PRIMARY KEY, email TEXT, firstname TEXT, lastname TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS companies (id TEXT PRIMARY KEY, name TEXT, domain TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS deals (id TEXT PRIMARY KEY, name TEXT, amount REAL, stage TEXT)")
    conn.commit()
    conn.close()

init_db()

# ------------------------
# Background fetch
# ------------------------
async def fetch(endpoint: str, method="get", body=None):
    async with httpx.AsyncClient() as client:
        r = await client.request(
            method,
            f"{BRIDGE_URL}{endpoint}",
            headers={"Authorization": BRIDGE_SECRET},
            json=body,
            timeout=60
        )
        return r.json()

async def sync_all():
    # Tickets (use schema-aligned endpoint: /tickets/search)
    data = await fetch("/tickets/search", method="post", body={"limit": 100})
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for t in data.get("results", []):
        tid = t["id"]
        subject = t.get("properties", {}).get("subject")
        createdate = t.get("properties", {}).get("createdate")
        company_id = None
        if "associations" in t and "companies" in t["associations"]:
            comps = t["associations"]["companies"]["results"]
            if comps:
                company_id = comps[0].get("id")
        cur.execute("INSERT OR REPLACE INTO tickets VALUES (?,?,?,?)", (tid, subject, createdate, company_id))
    conn.commit()
    conn.close()

    # TODO: extend with contacts, companies, deals using the new schema-aligned endpoints:
    # /contacts/get/{id}, /companies/get/{id}, /deals/get/{id}, etc.

# Run background sync every 10 mins
async def background_loop():
    while True:
        try:
            await sync_all()
        except Exception as e:
            print("Sync failed:", e)
        await asyncio.sleep(600)

@app.on_event("startup")
async def startup():
    asyncio.create_task(background_loop())

# ------------------------
# Learning endpoints
# ------------------------
@app.get("/learning/suggestions")
def suggestions():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    suggestions = []

    # Example: companies with lots of tickets
    cur.execute("SELECT company_id, COUNT(*) as c FROM tickets GROUP BY company_id HAVING c > 5")
    for row in cur.fetchall():
        suggestions.append({"type": "data_cleanup", "message": f"Company {row[0]} has {row[1]} tickets. Check CSM workload."})

    # Example: contacts with missing names
    cur.execute("SELECT id, email FROM contacts WHERE firstname IS NULL OR lastname IS NULL")
    for row in cur.fetchall():
        suggestions.append({"type": "data_cleanup", "message": f"Contact {row[1]} is missing profile details."})

    conn.close()
    return {"generated_at": datetime.utcnow().isoformat(), "suggestions": suggestions}

@app.get("/learning/kb-candidates")
def kb_candidates():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT subject FROM tickets ORDER BY createdate DESC LIMIT 20")
    rows = cur.fetchall()
    conn.close()
    return {"kb_candidates": [r[0] for r in rows if r[0]]}
