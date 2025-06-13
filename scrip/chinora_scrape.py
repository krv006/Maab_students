import pandas as pd
import requests
import time

base_url = "https://www.ctlottery.org/winners"
headers = {"User-Agent": "Mozilla/5.0"}

all_data = []
max_pages = 400
for page in range(1, max_pages + 1):
    url = f"{base_url}?page={page}"
    print(f"{page} downloading: {url}")

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        tables = pd.read_html(response.text)
        if tables:
            df = tables[0]
            if df.empty:
                print(f"Bosh page {page}")
                break
            all_data.append(df)
        else:
            print(f"Errors")
            break
        time.sleep(1)

    except Exception as e:
        print(f"❗ Xatolik sahifa {page} da: {e}")
        break

if all_data:
    full_df = pd.concat(all_data, ignore_index=True)
    full_df.to_excel("ct_lottery_all_pages.xlsx", index=False)
    print("DONE")
else:
    print("ERRORS")
