import asyncio, json, traceback
from pathlib import Path
from datetime import datetime

from telethon import TelegramClient, events
from telethon.sessions import StringSession


async def start_user_listener(phone, api_id, api_hash, session_string, groups):
    """
    Start a dedicated listener for a single user/session.

    - Uses StringSession correctly (no file hack).
    - Resolves each group id to an entity (works even if ids are positive).
    - Appends every new message to data_<phone>.json.
    - Prints detailed logs to Railway deploy logs for troubleshooting.
    """
    def log(*a):
        print(f"[{phone}]", *a, flush=True)

    try:
        # 1) Build client from StringSession (this is the critical fix)
        client = TelegramClient(StringSession(session_string), int(api_id), str(api_hash))
        await client.connect()

        # 2) Verify authorization
        if not await client.is_user_authorized():
            log("❌ Not authorized. The provided session_string is invalid or expired.")
            return

        me = await client.get_me()
        log(f"✅ Authorized as {getattr(me, 'first_name', '')} @{getattr(me, 'username', None)} (id={me.id})")

        # 3) Resolve groups to proper Telegram entities
        #    Accepts ints/strings; resolves each via get_entity
        resolved = []
        groups = [int(g) if isinstance(g, str) and g.isdigit() else g for g in groups]
        for g in groups:
            try:
                ent = await client.get_entity(g)
                resolved.append(ent)
                log(f"🧩 Resolved group {g} -> entity ok")
            except Exception as e:
                log(f"⚠️ Failed to resolve group {g}: {e}")

        if not resolved:
            log("⚠️ No valid groups resolved; listener will idle.")
        else:
            log(f"👂 Listening to {len(resolved)} chats")

        data_file = Path(f"data_{phone}.json")

        # 4) Event handler
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

                # Read-append-write JSON (simple and safe)
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

        # 5) Run forever
        await client.run_until_disconnected()

    except Exception as e:
        log("❌ Fatal listener error:", e, traceback.format_exc())
