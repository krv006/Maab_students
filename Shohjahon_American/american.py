import requests
import json

# API manzili
url = "https://nbreadymix.jonel.com:8443/goldgate/v1/ticket"

# So'rov parametrlar
params = {
    "AuthToken": "A7066CBF-1791-4C39-8968-B823A26953D3",
    "TicketId": 32249992,            # <-- BUNDA haqiqiy TicketId qo'yishing kerak
    "Signature": "false",          # <-- agar kerak bo‘lsa, Base64 imzoni ham oladi
    "ForcePricing": "false"        # <-- product narxlar ham keladi, agar kerak bo‘lsa
}

# GET so'rov yuborish
response = requests.get(url, params=params)

# Javobni tekshirish
if response.status_code == 200:
    print("Success: 200")
    data = response.json()

    # JSON faylga yozish
    with open("ticket_data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print("Ticket ma'lumotlari 'ticket_data.json' fayliga yozildi.")
else:
    print(f"Xatolik: {response.status_code} - {response.text}")
