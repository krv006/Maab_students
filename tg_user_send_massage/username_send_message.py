import asyncio
from telethon import TelegramClient
import os
import pandas as pd


API_ID = '27520896'
API_HASH = '1d60b08f0181d2be48ba0b64f2acb359'
PHONE_NUMBER = '901078055'

async def send_message_to_users():
    try:
        if API_ID is None or API_HASH is None or PHONE_NUMBER is None:
            raise ValueError("❌ Environment variables not loaded. Check your .env file!")

        try:
            df = pd.read_excel('username.xlsx')
            usernames = df['username'].tolist()
            usernames = [str(u).lstrip('@') for u in usernames if pd.notna(u) and str(u).strip()]
            usernames = usernames[:20]
        except FileNotFoundError:
            print("❌ username.xlsx file not found!")
            return
        except KeyError:
            print("❌ 'username' column not found in username.xlsx!")
            return

        async with TelegramClient("session_name", int(API_ID), API_HASH) as client:
            await client.start(PHONE_NUMBER)

            for username in usernames:
                try:
                    entity = await client.get_entity(username)
                    await client.send_message(entity, "Salom!")
                    print(f"✅ Sent 'Salom' to {username}")
                    await asyncio.sleep(1)
                except ValueError as e:
                    print(f"❌ Failed to send message to {username}: User not found or invalid username")
                except Exception as e:
                    print(f"❌ Error sending message to {username}: {str(e)}")

    except ValueError as e:
        print(e)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(send_message_to_users())