import requests
import json
import os
from datetime import datetime

API_LOGIN = "e8a769994d6948cbaca35c09b1345c71"
BASE_URL = "https://api-ru.iiko.services/api/1"
ORG_ID = "4d5955a5-3f97-4758-a705-f71f9dbf5968"

def get_token(api_login):
    url = f"{BASE_URL}/access_token"
    resp = requests.post(url, json={"apiLogin": api_login})
    if resp.status_code == 200:
        return resp.json().get("token")
    print("❌ Token error:", resp.status_code, resp.text)
    return None

def save_json(name, content, folder):
    with open(os.path.join(folder, f"{name}.json"), "w", encoding="utf-8") as f:
        json.dump(content, f, ensure_ascii=False, indent=2)

def main():
    token = get_token(API_LOGIN)
    if not token:
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_folder = f"iiko_export_{timestamp}"
    os.makedirs(export_folder, exist_ok=True)

    # 🧾 1. Order IDs olish
    order_list_resp = requests.post(BASE_URL + "/orders", headers=headers, json={
        "organizationIds": [ORG_ID],
        "withOrders": True,
        "includeDeleted": False
    })

    if order_list_resp.status_code != 200:
        print("❌ Buyurtmalarni olishda xatolik:", order_list_resp.text)
        return

    orders_data = order_list_resp.json()
    orders = orders_data.get("orders", [])
    order_ids = [order["id"] for order in orders]

    if not order_ids:
        print("❌ Hech qanday buyurtma topilmadi")
        return

    # 📦 2. order/by_id orqali to‘liq ma’lumot olish
    print("📥 order_by_id →", end=" ")
    payload = {
        "organizationIds": [ORG_ID],
        "orderIds": order_ids[:100]  # Birinchi 100 ta (xohlasang ko‘paytirasan)
    }

    resp = requests.post(BASE_URL + "/order/by_id", headers=headers, json=payload)
    if resp.status_code == 200:
        data = resp.json()
        save_json("order_by_id", data, export_folder)
        print("✅ saved")
    else:
        try:
            err = resp.json()
            print(f"❌ {resp.status_code} - {err.get('errorDescription') or json.dumps(err)[:200]}")
        except:
            print(f"❌ {resp.status_code} - {resp.text[:200]}")

if __name__ == "__main__":
    main()
