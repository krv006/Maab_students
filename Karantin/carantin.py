import json
import os

import pandas as pd
import requests

TOKEN = "040454c87ba748c3d00298fa18a803f631c6ddf1"

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

if os.path.exists("results.json"):
    with open("results.json", "r", encoding="utf-8") as f:
        old_results = json.load(f)
else:
    old_results = []

# Eski ma'lumotlarni dict shaklida tuzib olish (tez izlash uchun)
results_dict = {entry['inn']: entry['data'] for entry in old_results}

for inn in inn_list:
    url = f"http://dala.efito.uz/api/v1/fields/farmer/info/?inn={inn}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            results_dict[inn] = data
            print(f"[{inn}] - yangilandi")
        else:
            print(f"[{inn}] - status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"[{inn}] - xato : {e}")

updated_results = [{'inn': inn, 'data': data} for inn, data in results_dict.items()]

with open("results.json", "w", encoding="utf-8") as f:
    json.dump(updated_results, f, ensure_ascii=False, indent=4)

print("Barcha ma’lumotlar yangilandi ✅")
