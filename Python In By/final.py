import os

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

file_path = "ob_xavo.csv"


def get_region_code(region_name):
    region_codes = {
        "Andijon viloyati": "1703",
        "Buxoro viloyati": "1706",
        "Jizzax viloyati": "1708",
        "Qashqadaryo viloyati": "1710",
        "Navoiy viloyati": "1712",
        "Namangan viloyati": "1714",
        "Samarqand viloyati": "1718",
        "Surxandaryo viloyati": "1722",
        "Sirdaryo viloyati": "1724",
        "Toshkent viloyati": "1727",
        "Farg`ona viloyati": "1730",
        "Xorazm viloyati": "1733",
        "Qoraqalpog`iston respublikasi": "1735",
    }
    return region_codes.get(region_name, "Unknown")


def get_district_code(district_name):
    district_codes = {
        "Andijan": "1703203",
        "Bukhara": "1706207",
        "Jizzakh": "1708212",
        "Qarshi": "1710224",
        "Navoiy": "1712401",
        "Namangan": "1714212",
        "Samarkand": "1718233",
        "Termez": "1722220",
        "Gulistan": "1724220",
        "Fergana": "1730233",
        "Urgench": "1733217",
        "Nukus": "1735225",
        "Tashkent": "NULL",
        "Nurafshon": "1727206",
        "Kamchik": "NULL",
        "Chimgan": "NULL"
    }
    return district_codes.get(district_name, "Unknown")


district_region = {
    "tashkent": "Toshkent viloyati",
    "gulistan": "Sirdaryo viloyati",
    "urgench": "Xorazm viloyati",
    "nukus": "Qoraqalpog`iston respublikasi",
    "bukhara": "Buxoro viloyati",
    "navoiy": "Navoiy viloyati",
    "samarkand": "Samarqand viloyati",
    "jizzakh": "Jizzax viloyati",
    "qarshi": "Qashqadaryo viloyati",
    "termez": "Surxandaryo viloyati",
    "fergana": "Farg`ona viloyati",
    "namangan": "Namangan viloyati",
    "andijan": "Andijon viloyati",
    "nurafshon": "Toshkent viloyati",
    "kamchik": "Andijon viloyati",
    "chimgan": "Toshkent viloyati"
}

driver = webdriver.Chrome()
try:
    driver.get("https://hydromet.uz/")

    select_element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "select"))
    )
    select = Select(select_element)
    options = select.options

    data = {
        "district": [],
        "day": [],
        "day_part": [],
        "degree": [],
        "region": [],
        "region_code": [],
        "district_code": []
    }

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
                    data['district'].append(raw_district.upper())
                    data['day'].append(ls[0])
                    data['day_part'].append(ls[1])
                    data['degree'].append(ls[2].replace("…", "-"))
                    data['region'].append(region_name)
                    data['region_code'].append(region_code)
                    data['district_code'].append(district_code)
        except Exception as e:
            print(f"Error processing {district}: {e}")
            continue

    new_df = pd.DataFrame(data)

    # CSV mavjud bo‘lsa, o‘qiymiz
    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path)
    else:
        old_df = pd.DataFrame(columns=new_df.columns)

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
        else:
            merged_df = pd.concat([merged_df, pd.DataFrame([row])], ignore_index=True)

    merged_df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"✅ Data saved to {file_path}. Total rows: {len(merged_df)}")

finally:
    driver.quit()
