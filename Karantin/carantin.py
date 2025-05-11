import pandas as pd
import requests
import json

TOKEN = "040454c87ba748c3d00298fa18a803f631c6ddf1"


df = pd.read_excel("Test.xlsx")
inn_list = df['Tin'].dropna().astype(str).unique()

headers = {
    "Authorization": f"Token {TOKEN}"
}

results = []

for inn in inn_list:
    url = f"http://dala.efito.uz/api/v1/fields/farmer/info/?inn={inn}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            results.append({'inn': inn, 'data': data})
        else:
            print(f"[{inn}] - status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"[{inn}] - xato : {e}")

with open("results.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=4)

print("Done")
