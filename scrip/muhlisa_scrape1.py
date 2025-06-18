import requests
import pandas as pd
import json

DATA_URL = "https://smartup.online/b/anor/mxsx/mr/inventory$export"

cookies = {
    '_lrt': '1750230987989',
    'AMP_8db086350f': 'JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjIzNTYyMzA4Mi0zYzRiLTQyZmEtOGNiZS01Mzg1ZTdkYzQxNjIlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzUwMjMwOTg5NjY3JTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc1MDIzMDk4OTY3NCUyQyUyMmxhc3RFdmVudElkJTIyJTNBNiUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMSU3DA==',
    'biruni_device_id': 'EB45BB246B00670DFFE13902931B8B7D1E64D2CACC8BAFB46624D080CFED4A8D',
    'cw_conversation': 'eyJhbGciOiJIUzI1NiJ9.eyJzb3VyY2VfaWQiOiJlNzcyMjc2OS0zYWY0LTQ5NTgtYjJmMy1lNGVjMmM4MzcyZGMiLCJpbmJveF9pZCI6MX0.ojZPTcjIY8SWQAfqPcpE4KLF3hgP8Xgy0ri3HPyZ0xQ',
    'JSESSIONID': 'sx_app2~D1B32A53D6DB1C503EB75CAF7C97CAD9',
}

JSON_FILE = "smartup_export.json"

try:
    print("⬇️ 1. Ma'lumot yuklanmoqda...")
    response = requests.get(DATA_URL, cookies=cookies)
    response.raise_for_status()

    # Check content type
    content_type = response.headers.get('Content-Type', '').lower()
    print(f"📄 Content-Type: {content_type}")

    if 'application/json' in content_type or 'text/plain' in content_type:
        print("🔍 JSON format aniqlandi, JSON pars qilinmoqda...")
        # Parse JSON response
        data = response.json()

        # Check if the data has an "inventory" key
        if isinstance(data, dict) and "inventory" in data:
            # Flatten the nested JSON
            inventory = data["inventory"]
            df = pd.json_normalize(
                inventory,
                record_path="groups",  # Flatten the "groups" field
                meta=[
                    "product_id", "code", "name", "short_name", "weight_netto",
                    "weight_brutto", "litr", "box_type_code", "box_quant",
                    "producer_code", "measure_code", "state", "order_no",
                    "article_code", "barcodes", "gtin", "ikpu", "tnved",
                    "marking_group_code"
                ],  # Include top-level fields
                errors="ignore"
            )
        else:
            # If no "inventory" key, assume the data is a flat list
            df = pd.DataFrame(data)

        # Save DataFrame to JSON
        df.to_json(JSON_FILE, orient="records", indent=4, force_ascii=False)
        print(f"✅ JSON fayl saqlandi: {JSON_FILE}")
    else:
        print("❌ Kutilmagan content turi. HTML yoki JSON kutilgan edi.")
        # Save response for debugging
        with open("smartup_export.txt", "wb") as f:
            f.write(response.content)
        print("📜 Response smartup_export.txt ga saqlandi.")

except Exception as e:
    print(f"❌ Xatolik: {e}")