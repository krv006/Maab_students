import os
from datetime import datetime

import pandas as pd
import pyodbc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

# Fayl yo'li
file_path = "ob_xavo.csv"


# Viloyat kodlari
def get_region_code(region_name):
    region_codes = {
        "Andijon viloyati": "1703", "Buxoro viloyati": "1706", "Jizzax viloyati": "1708",
        "Qashqadaryo viloyati": "1710", "Navoiy viloyati": "1712", "Namangan viloyati": "1714",
        "Samarqand viloyati": "1718", "Surxandaryo viloyati": "1722", "Sirdaryo viloyati": "1724",
        "Toshkent viloyati": "1727", "Farg`ona viloyati": "1730", "Xorazm viloyati": "1733",
        "Qoraqalpog`iston respublikasi": "1735"
    }
    return region_codes.get(region_name, "Unknown")


# Tuman kodlari
def get_district_code(district_name):
    district_codes = {
        "Andijan": "1703203", "Bukhara": "1706207", "Jizzakh": "1708212", "Qarshi": "1710224",
        "Navoiy": "1712401", "Namangan": "1714212", "Samarkand": "1718233", "Termez": "1722220",
        "Gulistan": "1724220", "Fergana": "1730233", "Urgench": "1733217", "nukus": "1735225",
        "Tashkent": "NULL", "Nurafshon": "1727206", "Kamchik": "NULL", "Chimgan": "NULL"
    }
    return district_codes.get(district_name, "Unknown")


# Tuman-viloyat aloqasi
district_region = {
    "tashkent": "Toshkent viloyati", "gulistan": "Sirdaryo viloyati", "urgench": "Xorazm viloyati",
    "nukus": "Qoraqalpog`iston respublikasi", "bukhara": "Buxoro viloyati", "navoiy": "Navoiy viloyati",
    "samarkand": "Samarqand viloyati", "jizzakh": "Jizzax viloyati", "qarshi": "Qashqadaryo viloyati",
    "termez": "Surxandaryo viloyati", "fergana": "Farg`ona viloyati", "namangan": "Namangan viloyati",
    "andijan": "Andijon viloyati", "nurafshon": "Toshkent viloyati", "kamchik": "Andijon viloyati",
    "chimgan": "Toshkent viloyati"
}


# Sana formatlash funksiyasi
def format_date(date_str, current_year=2025):
    month_map = {
        "january": "01", "february": "02", "march": "03", "april": "04", "may": "05",
        "june": "06", "july": "07", "august": "08", "september": "09", "october": "10",
        "november": "11", "december": "12", "yanvar": "01", "fevral": "02", "mart": "03",
        "aprel": "04", "may": "05", "iyun": "06", "iyul": "07", "avgust": "08",
        "sentyabr": "09", "oktyabr": "10", "noyabr": "11", "dekabr": "12"
    }
    try:
        parts = date_str.lower().strip().split()
        if len(parts) < 2:
            return "Unknown"
        day, month = parts[0], parts[1]
        month_num = month_map.get(month.lower(), "Unknown")
        if month_num == "Unknown":
            return "Unknown"
        day = day.zfill(2)
        return f"{day}-{month_num}-{current_year}"
    except Exception as e:
        print(f"Error parsing date '{date_str}': {e}")
        return "Unknown"


def main():
    # Joriy sanani olish
    current_date = datetime.now()
    current_date_str = current_date.strftime("%d-%m-%Y")

    # Ma'lumotlar bazasiga ulanish
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=192.168.111.14;"
        "DATABASE=weather;"
        "UID=sa;"
        "PWD=AX8wFfMQrR6b9qdhHt2eYS;"
    )
    cursor = conn.cursor()

    # Selenium brauzerini ishga tushirish
    driver = webdriver.Chrome()
    try:
        driver.get("https://hydromet.uz/")
        select = Select(WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "select"))
        ))
        options = select.options

        # Ma'lumotlar uchun konteyner
        data = {
            "district": [], "day": [], "day_part": [], "degree": [],
            "region": [], "region_code": [], "district_code": []
        }

        # Har bir tuman uchun ma'lumot yig'ish
        for option in options:
            raw_district = option.text.strip()
            if not raw_district:
                continue

            district = raw_district.title()
            region_name = district_region.get(district.lower(), "Unknown")
            region_code = get_region_code(region_name)
            district_code = get_district_code(district)

            try:
                select.select_by_visible_text(raw_district)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".weather-widget__forecast__table .day"))
                )
                blocks = driver.find_elements(By.CSS_SELECTOR, ".weather-widget__forecast__table .day")

                for block in blocks:
                    ls = block.text.strip().split("\n")
                    if len(ls) >= 3:
                        formatted_date = format_date(ls[0])
                        if formatted_date == "Unknown":
                            print(f"Skipping invalid date for {raw_district}: {ls[0]}")
                            continue

                        # Faqat joriy kun va undan keyingi sanalarni qayta ishlash
                        date_obj = datetime.strptime(formatted_date, "%d-%m-%Y")
                        if date_obj.date() >= current_date.date():
                            data['district'].append(raw_district.upper())
                            data['day'].append(formatted_date)
                            data['day_part'].append(ls[1])
                            data['degree'].append(ls[2].replace("…", "-"))
                            data['region'].append(region_name)
                            data['region_code'].append(region_code)
                            data['district_code'].append(district_code)
            except Exception as e:
                print(f"Error processing {district}: {e}")
                continue

        # Ma'lumotlarni DataFrame'ga aylantirish
        new_df = pd.DataFrame(data)
        if new_df.empty:
            print("No data collected.")
            return

        # Ma'lumot turlarini aniqlashtirish
        new_df = new_df.astype({
            "district": str, "day": str, "day_part": str, "degree": str,
            "region": str, "region_code": str, "district_code": str
        })

        # Ma'lumotlar bazasiga yozish
        for _, row in new_df.iterrows():
            cursor.execute("""
                SELECT degree FROM hydromet 
                WHERE district = ? AND day = ? AND day_part = ?
            """, (row['district'], row['day'], row['day_part']))
            result = cursor.fetchone()

            if result:
                if result[0] != row['degree']:
                    cursor.execute("""
                        UPDATE hydromet
                        SET degree = ?, region = ?, region_code = ?, district_code = ?
                        WHERE district = ? AND day = ? AND day_part = ?
                    """, (row['degree'], row['region'], row['region_code'], row['district_code'],
                          row['district'], row['day'], row['day_part']))
                    print(f"Updated: {row['district']} on {row['day']} ({row['day_part']}): {row['degree']}")
            else:
                cursor.execute("""
                    INSERT INTO hydromet (district, day, day_part, degree, region, region_code, district_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (row['district'], row['day'], row['day_part'], row['degree'],
                      row['region'], row['region_code'], row['district_code']))
                print(f"Added: {row['district']} on {row['day']} ({row['day_part']})")

        conn.commit()

        # CSV faylga saqlash
        if os.path.exists(file_path):
            old_df = pd.read_csv(file_path, dtype={
                "district": str, "day": str, "day_part": str, "degree": str,
                "region": str, "region_code": str, "district_code": str
            })
        else:
            old_df = pd.DataFrame(columns=new_df.columns).astype({
                "district": str, "day": str, "day_part": str, "degree": str,
                "region": str, "region_code": str, "district_code": str
            })

        # Joriy kun va undan keyingi kunlar uchun eski ma'lumotlarni yangilash
        merged_df = old_df.copy()
        for _, row in new_df.iterrows():
            mask = (
                    (merged_df['district'] == row['district']) &
                    (merged_df['day'] == row['day']) &
                    (merged_df['day_part'] == row['day_part'])
            )
            if mask.any():
                if merged_df.loc[mask, 'degree'].values[0] != row['degree']:
                    merged_df.loc[mask, 'degree'] = row['degree']
                    merged_df.loc[mask, 'region'] = row['region']
                    merged_df.loc[mask, 'region_code'] = row['region_code']
                    merged_df.loc[mask, 'district_code'] = row['district_code']
                    print(f"CSV Updated: {row['district']} on {row['day']} ({row['day_part']})")
            else:
                merged_df = pd.concat([merged_df, pd.DataFrame([row])], ignore_index=True)
                print(f"CSV Added: {row['district']} on {row['day']} ({row['day_part']})")

        merged_df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"✅ Data saved to {file_path}. Total rows: {len(merged_df)}")

    finally:
        driver.quit()
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
