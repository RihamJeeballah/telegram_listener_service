from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import asyncio, json
from pathlib import Path
from listener_worker import start_user_listener

# ✅ FastAPI app (مهم جدًا أن يكون بهذا الاسم بالضبط)
app = FastAPI(title="Telegram Listener Service")

DB_FILE = Path("database.json")
LISTENERS = {}

def load_db():
    if DB_FILE.exists():
        return json.loads(DB_FILE.read_text())
    return {}

def save_db(data):
    DB_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))

class StartRequest(BaseModel):
    phone: str
    api_id: str
    api_hash: str
    session_string: str
    groups: list[int]

@app.post("/start_listener")
async def start_listener(req: StartRequest, background_tasks: BackgroundTasks):
    """تشغيل listener لكل مستخدم"""
    db = load_db()
    db[req.phone] = {
        "api_id": req.api_id,
        "api_hash": req.api_hash,
        "session_string": req.session_string,
        "groups": req.groups,
    }
    save_db(db)

    if req.phone not in LISTENERS:
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            start_user_listener(req.phone, req.api_id, req.api_hash, req.session_string, req.groups)
        )
        LISTENERS[req.phone] = task

    return {"status": "started", "user": req.phone}

@app.get("/get_data/{phone}")
def get_data(phone: str):
    """إرجاع الرسائل المستخرجة للمستخدم"""
    data_file = Path(f"data_{phone}.json")
    if not data_file.exists():
        return []
    return json.loads(data_file.read_text())
