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
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "600"))  # every 10 minutes

# ========================================
# DB Setup
# ========================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id TEXT PRIMARY KEY,
            subject TEXT,
            content TEXT,
            created_at TEXT,
            last_seen TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_ticket(ticket_id, subject, content):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO tickets (ticket_id, subject, content, created_at, last_seen)
        VALUES (?, ?, ?, COALESCE((SELECT created_at FROM tickets WHERE ticket_id = ?), ?), ?)
    """, (ticket_id, subject, content, ticket_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def get_ticket_count():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM tickets")
    count = c.fetchone()[0]
    conn.close()
    return count

def get_common_subjects(limit=5):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT subject, COUNT(*) as freq
        FROM tickets
        WHERE subject IS NOT NULL
        GROUP BY subject
        ORDER BY freq DESC
        LIMIT ?
    """, (limit,))
    rows = c.fetchall()
    conn.close()
    return [{"subject": r[0], "count": r[1]} for r in rows]

# ========================================
# FastAPI
# ========================================
app = FastAPI(title="HubSpot Learner Service")

@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(learning_loop())

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/learning/status")
def learning_status():
    return {"tickets_indexed": get_ticket_count()}

@app.get("/learning/suggestions")
def learning_suggestions():
    subjects = get_common_subjects()
    return {
        "message": "Most common ticket subjects (good candidates for KB articles)",
        "subjects": subjects
    }

# ========================================
# Background Loop
# ========================================
async def learning_loop():
    while True:
        try:
            print("[Learner] Polling for tickets...")
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    f"{CONNECTOR_URL}/tickets/search",
                    params={"limit": 20, "properties": ["subject", "content"]},
                    headers={"Authorization": BRIDGE_SECRET},
                    timeout=60
                )
                if r.status_code == 200:
                    data = r.json()
                    results = data.get("results", [])
                    for t in results:
                        ticket_id = t.get("id")
                        props = t.get("properties", {})
                        subject = props.get("subject")
                        content = props.get("content")
                        if ticket_id:
                            save_ticket(ticket_id, subject, content)
                            print(f"[Learner] Saved ticket {ticket_id}")
                else:
                    print(f"[Learner] Error fetching tickets: {r.status_code} {r.text}")
        except Exception as e:
            print(f"[Learner] Exception: {e}")

        await asyncio.sleep(POLL_INTERVAL)
