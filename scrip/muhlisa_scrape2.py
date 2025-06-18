import requests
import pandas as pd

DATA_URL = "https://smartup.online/b/anor/mxsx/mr/inventory$export"

cookies = {
    '_lrt': '1750230987989',
    'AMP_8db086350f': 'JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjIzNTYyMzA4Mi0zYzRiLTQyZmEtOGNiZS01Mzg1ZTdkYzQxNjIlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzUwMjMwOTg5NjY3JTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc1MDIzMDk4OTY3NCUyQyUyMmxhc3RFdmVudElkJTIyJTNBNiUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMSU3RA==',
    'biruni_device_id': 'EB45BB246B00670DFFE13902931B8B7D1E64D2CACC8BAFB46624D080CFED4A8D',
    'cw_conversation': 'eyJhbGciOiJIUzI1NiJ9.eyJzb3VyY2VfaWQiOiJlNzcyMjc2OS0zYWY0LTQ5NTgtYjJmMy1lNGVjMmM4MzcyZGMiLCJpbmJveF9pZCI6MX0.ojZPTcjIY8SWQAfqPcpE4KLF3hgP8Xgy0ri3HPyZ0xQ',
    'JSESSIONID': 'sx_app2~D1B32A53D6DB1C503EB75CAF7C97CAD9',
}

HTML_FILE = "smartup_export.html"
JSON_FILE = "smartup_export.json"

try:
    print("⬇️ 1. Ma'lumot yuklanmoqda (HTML)...")
    response = requests.get(DATA_URL, cookies=cookies)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "").lower()
    print(f"ℹ️ Content-Type: {content_type}")

    if "html" not in content_type:
        raise Exception("❌ Kutilgan HTML emas, balki boshqa format qaytdi. To‘g‘ri endpoint yoki cookie'lar ekanligini tekshiring.")

    with open(HTML_FILE, "wb") as f:
        f.write(response.content)
    print(f"✅ HTML fayl saqlandi: {HTML_FILE}")

    print("🔁 2. HTML dan jadval olinmoqda...")

    try:
        df_list = pd.read_html(HTML_FILE, flavor="lxml")
        if not df_list:
            raise ValueError("Hech qanday jadval topilmadi.")
        df = df_list[0]
    except Exception as e:
        raise Exception("❌ HTML fayl ichida <table> topilmadi yoki noto‘g‘ri format. Sayt login sahifani qaytargan bo‘lishi mumkin.")

    df.to_json(JSON_FILE, orient="records", indent=4, force_ascii=False)
    print(f"✅ JSON fayl saqlandi: {JSON_FILE}")

except requests.exceptions.RequestException as req_err:
    print(f"❌ HTTP xato: {req_err}")
except Exception as err:
    print(f"❌ Xatolik: {err}")
