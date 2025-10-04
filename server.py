from fastapi import FastAPI
from pydantic import BaseModel
import asyncio, json
from pathlib import Path
from listener_worker import start_user_listener

app = FastAPI(title="Telegram Listener Service")

DB_FILE = Path("database.json")
LISTENERS: dict[str, asyncio.Task] = {}


def load_db():
    if DB_FILE.exists():
        return json.loads(DB_FILE.read_text(encoding="utf-8"))
    return {}


def save_db(data):
    DB_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


class StartRequest(BaseModel):
    phone: str
    api_id: str
    api_hash: str
    session_string: str
    groups: list[int] | list[str]


@app.post("/start_listener")
async def start_listener(req: StartRequest):
    """Start (or restart) a background listener for this user/session."""
    db = load_db()
    db[req.phone] = {
        "api_id": req.api_id,
        "api_hash": req.api_hash,
        "session_string": req.session_string,
        "groups": req.groups,
    }
    save_db(db)

    # If there is a running task for this phone, keep it (or cancel & restart)
    if req.phone in LISTENERS:
        task = LISTENERS[req.phone]
        if task.done() or task.cancelled():
            # Clean up and recreate
            pass
        else:
            # Already running; just acknowledge and return
            return {"status": "already_running", "user": req.phone}

    loop = asyncio.get_event_loop()
    task = loop.create_task(
        start_user_listener(req.phone, req.api_id, req.api_hash, req.session_string, req.groups)
    )
    LISTENERS[req.phone] = task
    return {"status": "started", "user": req.phone}


@app.get("/get_data/{phone}")
def get_data(phone: str):
    """Return collected raw messages for this user."""
    data_file = Path(f"data_{phone}.json")
    if not data_file.exists():
        return []
    try:
        return json.loads(data_file.read_text(encoding="utf-8"))
    except Exception:
        return []
