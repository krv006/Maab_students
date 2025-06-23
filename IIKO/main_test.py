import requests
import json
import os
from datetime import datetime

API_LOGIN = "e8a769994d6948cbaca35c09b1345c71"
BASE_URL = "https://api-ru.iiko.services/api/1"
ORG_IDS = [
    "4d5955a5-3f97-4758-a705-f71f9dbf5968",
    "989380d8-5a1c-4379-8a54-84ec7128d336"
]
ORG_ID = ORG_IDS[0]

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
    export_folder = f"iiko_full_export_{timestamp}"
    os.makedirs(export_folder, exist_ok=True)

    endpoints = {
        "organizations":                   ("/organizations",            {}),
        "terminal_groups":                ("/terminal_groups",         {"organizationIds": ORG_IDS}),
        "nomenclature":                   ("/nomenclature",            {"organizationId": ORG_ID, "startRevision": 0}),
        "stop_lists":                     ("/stop_lists",              {"organizationIds": ORG_IDS}),
        "payment_types":                  ("/payment_types",           {"organizationIds": ORG_IDS}),
        "discounts":                      ("/discounts",               {"organizationIds": ORG_IDS}),
        "removal_types":                  ("/removal_types",           {"organizationIds": ORG_IDS}),
        "deliveries_order_types":         ("/deliveries/order_types",  {"organizationIds": ORG_IDS}),
        "cancel_causes":                  ("/cancel_causes",           {"organizationIds": ORG_IDS}),
        "cities":                         ("/cities",                  {"organizationIds": ORG_IDS}),
        "regions":                        ("/regions",                 {"organizationIds": ORG_IDS}),
        "courier_locations":              ("/employees/couriers/locations/by_time_offset", {
            "organizationIds": ORG_IDS,
            "startTime": "2025-06-01T00:00:00.000Z",
            "finishTime": "2025-06-18T00:00:00.000Z"
        }),
        "orders":                         ("/orders", {
            "organizationIds": [ORG_ID],
            "withOrders": True,
            "includeDeleted": False
        }),
        "streets_by_city":                ("/streets/by_city", {
            "organizationId": ORG_ID,
            "cityId": ""  # Will be set below
        })
    }

    # Get first cityId to inject for streets_by_city
    try:
        cities = requests.post(BASE_URL + "/cities", headers=headers, json={"organizationIds": ORG_IDS}).json()
        endpoints["streets_by_city"][1]["cityId"] = cities["cities"][0]["items"][0]["id"]
    except Exception as e:
        print(f"Failed to set cityId: {e}")

    for name, (path, payload) in endpoints.items():
        try:
            print(f"{name} →", end=" ")
            resp = requests.post(BASE_URL + path, headers=headers, json=payload)
            if resp.status_code == 200:
                data = resp.json()
                save_json(name, data, export_folder)
                print("✅ saved")
            else:
                try:
                    err = resp.json()
                    print(f"❌ {resp.status_code} - {err.get('errorDescription') or json.dumps(err)[:200]}")
                except:
                    print(f"❌ {resp.status_code} - {resp.text[:200]}")
        except Exception as e:
            print(f"❌ {name} → failed: {e}")

if __name__ == "__main__":
    main()
