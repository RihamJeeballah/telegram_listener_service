# listener_worker.py
import asyncio, json, traceback, os
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Set

from telethon import TelegramClient, events
from telethon.sessions import StringSession


DATA_DIR = Path(os.getenv("DATA_DIR", "/data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _atomic_write_json(path: Path, data: Dict | list):
    """Write JSON atomically to avoid partial files on restarts."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


async def start_user_listener(phone: str, api_id: int, api_hash: str, session_string: str, groups):
    """
    Start a dedicated listener for one user.
    - Uses StringSession correctly
    - Resolves provided group IDs against the user's dialogs to obtain full entities
    - Appends every new message to /DATA_DIR/data_<phone>.json (atomic), with de-duplication
    """
    def log(*a):
        print(f"[{phone}]", *a, flush=True)

    client: TelegramClient | None = None
    try:
        # 1) Build client from StringSession
        client = TelegramClient(StringSession(session_string), int(api_id), str(api_hash))
        await client.connect()

        # 2) Verify session
        if not await client.is_user_authorized():
            log("❌ Not authorized. Invalid/expired session_string.")
            return

        me = await client.get_me()
        log(f"✅ Authorized as {getattr(me, 'first_name', '')} @{getattr(me, 'username', None)} (id={me.id})")

        # 3) Normalize incoming group IDs (strings -> ints)
        norm_ids = []
        for g in groups or []:
            try:
                norm_ids.append(int(g))
            except Exception:
                log(f"⚠️ Skipping non-integer group id: {g}")

        # 4) Fetch all dialogs once and build a map id -> entity (has access_hash)
        dialogs = await client.get_dialogs(limit=None)
        by_id = {}
        for d in dialogs:
            ent = getattr(d, "entity", None)
            if ent is not None and hasattr(ent, "id"):
                by_id[getattr(ent, "id")] = ent

        resolved = []
        for gid in norm_ids:
            ent = by_id.get(gid)
            if ent is not None:
                resolved.append(ent)
                log(f"🧩 Resolved group {gid} -> {type(ent).__name__}")
            else:
                log(f"⚠️ Could not resolve group {gid} from dialogs cache. "
                    f"Are you a member of this group on this account?")

        data_file = DATA_DIR / f"data_{phone}.json"
        # Load existing data & build a quick de-dup set
        existing: list = []
        seen: Set[Tuple[int, int]] = set()  # (chat_id, message_id)
        if data_file.exists():
            try:
                existing = json.loads(data_file.read_text(encoding="utf-8")) or []
                for r in existing:
                    cid = r.get("chat_id")
                    mid = r.get("message_id")
                    if cid is not None and mid is not None:
                        seen.add((int(cid), int(mid)))
                log(f"📦 Loaded {len(existing)} existing messages.")
            except Exception as e:
                log(f"⚠️ Failed reading existing data file: {e}")

        collected = existing  # alias

        if not resolved:
            log("⚠️ No valid groups resolved; listener will idle (no handler attached).")
            # Idle: just keep the connection alive so the task remains running
            await client.run_until_disconnected()
            return

        log(f"👂 Listening to {len(resolved)} chat(s).")
        lock = asyncio.Lock()  # serialize file writes

        @client.on(events.NewMessage(chats=resolved))
        async def handler(event):
            try:
                msg_text = event.raw_text or ""
                cid = int(event.chat_id)
                mid = int(event.id)
                key = (cid, mid)
                if key in seen:
                    return  # de-dup

                rec = {
                    "timestamp": event.date.replace(tzinfo=None).isoformat(),
                    "text": msg_text,
                    "chat_id": cid,
                    "message_id": mid,
                }

                async with lock:
                    collected.append(rec)
                    seen.add(key)
                    _atomic_write_json(data_file, collected)

                log(f"💾 Captured msg #{mid} from chat {cid}")

            except Exception as e:
                log("Handler error:", e, traceback.format_exc())

        await client.run_until_disconnected()

    except asyncio.CancelledError:
        log("⚠️ Listener task cancelled.")
        raise
    except Exception as e:
        log("❌ Fatal listener error:", e, traceback.format_exc())
    finally:
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
