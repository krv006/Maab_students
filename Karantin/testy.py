import pandas as pd
import requests
import json
import os

TOKEN = "040454c87ba748c3d00298fa18a803f631c6ddf1"

# Excel fayldan TINlarni olish
df = pd.read_excel("Test.xlsx")
inn_list = (
    df['Tin']
    .dropna()
    .astype(float)
    .astype(int)
    .astype(str)
    .unique()
)

headers = {
    "Authorization": f"Token {TOKEN}"
}

# Eski JSON faylni o‘qish (agar mavjud bo‘lsa)
if os.path.exists("results.json"):
    with open("results.json", "r", encoding="utf-8") as f:
        old_results = json.load(f)
else:
    old_results = []

# Eski inn lar ro‘yxati
existing_inns = {entry['inn'] for entry in old_results}

# Yangi ma'lumotlarni yig‘ish
new_results = []

for inn in inn_list:
    if inn in existing_inns:
        print(f"[{inn}] - allaqachon mavjud, o‘tkazildi")
        continue

    url = f"http://dala.efito.uz/api/v1/fields/farmer/info/?inn={inn}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            new_results.append({'inn': inn, 'data': data})
        else:
            print(f"[{inn}] - status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"[{inn}] - xato : {e}")

# Eski va yangi ma'lumotlarni birlashtirish
all_results = old_results + new_results

# JSON faylga saqlash
with open("results.json", "w", encoding="utf-8") as f:
    json.dump(all_results, f, ensure_ascii=False, indent=4)

print("Yangi ma’lumotlar qo‘shildi ✅")
