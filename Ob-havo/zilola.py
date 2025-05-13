from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd

regions = [
    {"Andijon viloyati": 1703, "url": "andijan"},
    {"Buxoro viloyati": 1706, "url": "bukhara"},
    {"Farg`ona": 1730, "url": "fergana"},
    {"Jizzax viloyati": 1708, "url": "jizzakh"},
    {"Namangan viloyati": 1714, "url": "namangan"},
    {"Navoiy viloyati": 1712, "url": "navoiy"},
    {"Qashqadaryo viloyati": 1710, "url": "qashqadaryo"},
    {"Qoraqalpog`iston Respublikasi": 1735, "url": "karakalpakstan"},
    {"Samarqand viloyati": 1718, "url": "samarqand/samarkand"},
    {"Sirdaryo viloyati": 1724, "url": "sirdaryo"},
    {"Surxandaryo": 172, "url": "surkhondaryo"},
    {"Toshkent viloyati": 1727, "url": "tashkent"},
    {"Xorazm viloyati": 1733, "url": "xorazm"}
]

base_url = "https://www.ob-havo.com/asia/uzbekistan/"
weather_data = []

def extract_weather_data(region_name, region_code, region_url, date_type, real_date=None):
    if date_type == "today":
        url = f"{base_url}{region_url}?page=today"
        formatted_date = real_date.strftime('%Y-%m-%d')
    elif date_type == "tomorrow":
        url = f"{base_url}{region_url}?page=tomorrow"
        formatted_date = (real_date + timedelta(days=1)).strftime('%Y-%m-%d')
    elif date_type == "next_day":
        target_date = real_date + timedelta(days=2)
        formatted_date = target_date.strftime('%Y-%m-%d')
        url = f"{base_url}{region_url}?page=day&date={formatted_date}"  # ✅ TUZATILDI
    else:
        return

    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        forecast_items = soup.select("#day-table td .day_temp .day-value")
        weather_conditions = soup.select("#day-table td.des_td .weather_des")

        if len(forecast_items) < 8 or len(weather_conditions) < 3:
            print(f"Ma'lumot topilmadi: {region_name} ({date_type})")
            return

        # Morning
        morning_range = f"{min(i.text for i in forecast_items[2:7])}-{max(i.text for i in forecast_items[2:7])}"
        morning_weather = weather_conditions[2].text.strip()

        # Night
        evening_indices = [7, 0, 1]
        evening_range = f"{min(forecast_items[i].text for i in evening_indices)}-{max(forecast_items[i].text for i in evening_indices)}"
        evening_weather = weather_conditions[0].text.strip()

        weather_data.append({
            "region_name": region_name,
            "region_code": region_code,
            "time_of_day": "morning",
            "temperature": morning_range,
            "weather_condition": morning_weather,
            "date": formatted_date
        })
        weather_data.append({
            "region_name": region_name,
            "region_code": region_code,
            "time_of_day": "night",
            "temperature": evening_range,
            "weather_condition": evening_weather,
            "date": formatted_date
        })

    except Exception as e:
        print(f"Error: {region_name} ({date_type}): {e}")

# === MAIN LOOP ===
today = datetime.today()
for region in regions:
    region_name = [k for k in region if k != "url"][0]
    region_code = region[region_name]
    region_url = region["url"]
    print(f"⏳ Processing {region_name}...")

    extract_weather_data(region_name, region_code, region_url, "today", today)
    extract_weather_data(region_name, region_code, region_url, "tomorrow", today)
    extract_weather_data(region_name, region_code, region_url, "next_day", today)

# Save to Excel
df = pd.DataFrame(weather_data)
df.to_excel("uzbekistan_weather_3days.xlsx", index=False)
print("✅ Saqlandi: uzbekistan_weather_3days.xlsx")
