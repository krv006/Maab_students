from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
import asyncio
import re

# API ma'lumotlari (Yangi api_id va api_hash bilan almashtiring)
api_id = 12345678  # Yangisini qo'ying
api_hash = '32 sonlar va raqamlardan iborat kod'  # Yangisini qo'ying
phone = '+998#########'

# TelegramClient yaratish
client = TelegramClient('session_name', api_id, api_hash)

# 2FA parolni oldindan belgilash (xavfsizlik uchun buni o'zgartiring)
two_fa_password = "your phone number"  # Bu yerga o'zingizning 2FA parolingizni qo'ying


# Telegramdan kelgan kodni avtomatik olish uchun event handler
async def get_code_from_message():
    code = None

    @client.on(events.NewMessage(incoming=True, from_users='Telegram'))
    async def handler(event):
        nonlocal code
        message = event.message.message
        # Kod 5 raqamli bo'lib keladi, shuni regex bilan qidiramiz
        match = re.search(r'\b\d{5}\b', message)
        if match:
            code = match.group(0)
            print(f"Topilgan kod: {code}")

    # Kod kelguncha kutamiz (10 soniya ichida kelishi kerak)
    for _ in range(10):
        if code:
            return code
        await asyncio.sleep(1)
    raise Exception("Telegramdan kod kelmadi, qo'lda kiriting.")


async def main():
    try:
        # Clientni ishga tushirish
        async with client:
            print("Ulanish boshlandi...")
            # Avtomatik autentifikatsiya
            await client.start(phone=phone, code_callback=get_code_from_message, password=two_fa_password)
            print("Muvaffaqiyatli ulandi!")

            # Kontaktlar ro'yxati
            contacts = ['user name or phone number']
            message = "Salom! Bu Python'dan yuborilgan test xabar."

            # Har bir kontakga xabar yuborish
            for contact in contacts:
                try:
                    await client.send_message(contact, message)
                    print(f"Xabar {contact} ga yuborildi")
                except ValueError as e:
                    print(f"{contact} topilmadi: {e}")
                except Exception as e:
                    print(f"{contact} ga xabar yuborishda xatolik: {e}")
                await asyncio.sleep(1)  # Spam filtridan qochish uchun pauza

            # Kontaktlarni ko'rish (ixtiyoriy)
            contacts_response = await client.get_contacts()
            for user in contacts_response:
                username = f"@{user.username}" if user.username else "Username yo'q"
                phone_num = user.phone if user.phone else "Telefon yo'q"
                print(f"Kontakt: {phone_num} - {username}")

    except SessionPasswordNeededError:
        print("2FA parol noto'g'ri. Qo'lda kiriting yoki 'two_fa_password' ni yangilang.")
    except Exception as e:
        print(f"Umumiy xatolik yuz berdi: {e}")


# Jupyter Notebook uchun ishga tushirish
await main()
