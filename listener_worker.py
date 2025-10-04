# listener_worker.py
import asyncio, json, traceback
from pathlib import Path
from datetime import datetime

from telethon import TelegramClient, events
from telethon.sessions import StringSession


async def start_user_listener(phone, api_id, api_hash, session_string, groups):
    """
    Start a dedicated listener for one user.
    - Uses StringSession correctly
    - Resolves provided group IDs against the user's dialogs to obtain full entities
    - Appends every new message to data_<phone>.json
    """
    def log(*a):
        print(f"[{phone}]", *a, flush=True)

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
        #    This is the key fix so Telethon can resolve channels/supergroups.
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

        if not resolved:
            log("⚠️ No valid groups resolved; listener will idle.")
        else:
            log(f"👂 Listening to {len(resolved)} chat(s).")

        data_file = Path(f"data_{phone}.json")

        @client.on(events.NewMessage(chats=resolved if resolved else None))
        async def handler(event):
            try:
                msg_text = event.raw_text or ""
                msg = {
                    "timestamp": event.date.replace(tzinfo=None).isoformat(),
                    "text": msg_text,
                    "chat_id": event.chat_id,
                    "message_id": event.id,
                }

                # append to JSON file
                data = []
                if data_file.exists():
                    try:
                        data = json.loads(data_file.read_text(encoding="utf-8"))
                    except Exception:
                        data = []
                data.append(msg)
                data_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                log(f"💾 Captured msg #{event.id} from chat {event.chat_id}")

            except Exception as e:
                log("Handler error:", e, traceback.format_exc())

        await client.run_until_disconnected()

    except Exception as e:
        log("❌ Fatal listener error:", e, traceback.format_exc())
