from dotenv import load_dotenv
import asyncio
from telethon import TelegramClient
import os

load_dotenv()

API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE_NUMBER = os.getenv('PHONE_NUMBER')


try:
    if API_ID is None or API_HASH is None or PHONE_NUMBER is None:
        raise ValueError("❌ Environment variables not loaded. Check your .env file!")

    users_id = {1305675046, 237282713, 5663079735}

    async def send_message_to_private_chats():
        async with TelegramClient("session_name", int(API_ID), API_HASH) as client:
            await client.start(PHONE_NUMBER)

            dialogs = await client.get_dialogs()
            for dialog in dialogs:
                if dialog.is_user:
                    user_id = dialog.entity.id
                    if user_id in users_id:
                        await client.send_message(user_id, "Salom!")  # Send message
                        print(f"✅ Sent 'Salom' to {dialog.entity.first_name}")
    asyncio.run(send_message_to_private_chats())
except ValueError as e:
    print(e)
    
