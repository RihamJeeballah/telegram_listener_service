import asyncio, json
from pathlib import Path
from telethon import TelegramClient, events
from datetime import datetime

async def start_user_listener(phone, api_id, api_hash, session_string, groups):
    """Listener لكل مستخدم"""
    session_file = Path(f"{phone}.session")
    session_file.write_text(session_string)  # تخزين جلسة المستخدم

    client = TelegramClient(str(session_file), int(api_id), api_hash)
    await client.start()

    data_file = Path(f"data_{phone}.json")

    @client.on(events.NewMessage(chats=groups))
    async def handler(event):
        msg = {
            "timestamp": datetime.utcnow().isoformat(),
            "text": event.raw_text,
        }
        if data_file.exists():
            data = json.loads(data_file.read_text())
        else:
            data = []
        data.append(msg)
        data_file.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        print(f"[{phone}] رسالة جديدة من {event.chat_id}")

    print(f"✅ Listener شغال للمستخدم {phone} في {len(groups)} قروب(ات)")
    await client.run_until_disconnected()
