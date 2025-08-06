import json

import requests

url = "https://nbreadymix.jonel.com:8443/goldgate/v1/order"

params = {
    "AuthToken": "A7066CBF-1791-4C39-8968-B823A26953D3",
    "OrderId": 455398
}

response = requests.get(url, params=params)

if response.status_code == 200:
    print("Success: 200")
    data = response.json()
    with open("order_data2.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print("Order ma'lumotlari 'order_data2.json' fayliga yozildi.")
else:
    print(f"Xatolik: {response.status_code} - {response.text}")
