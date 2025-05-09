import os

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait

file_path = "Nodirxon.csv"


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


def get_district_code(city_name):
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
        "Tashkent": "Unknown",
        "Nurafshon": "1727206",
        "Kamchik": "Unknown",
        "Chimgan": "Unknown"
    }
    return district_codes.get(city_name, "Unknown")


city_region = {
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

    try:
        select_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "select"))
        )
        print("Select element found")
    except Exception as e:
        print(f"Error finding select element: {e}")
        driver.quit()
        exit()

    select = Select(select_element)
    options = select.options
    print(f"Found {len(options)} city options")

    data = {
        "city": [],
        "day": [],
        "day_part": [],
        "degree": [],
        "region": [],
        "region_code": [],
        "district_code": []
    }

    for option in options:
        try:
            raw_city = option.text.strip()
            city = raw_city.title()  # Normalize for mappings
            if not raw_city:
                print("Skipping empty city option")
                continue
            print(f"Processing city: {city} (raw: {raw_city})")

            select.select_by_visible_text(raw_city)

            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".weather-widget__forecast__table .day"))
            )

            blocks = driver.find_elements(By.CSS_SELECTOR, ".weather-widget__forecast__table .day")
            print(f"Found {len(blocks)} forecast blocks for {city}")

            for block in blocks:
                ls = block.text.split("\n")
                print(f"Block data: {ls}")
                if len(ls) >= 3:
                    region_name = city_region.get(city.lower(), "Unknown")
                    data['city'].append(raw_city.upper())  # Uppercase for output
                    data['day'].append(ls[0])
                    data['day_part'].append(ls[1])
                    data['degree'].append(ls[2].replace("…", "-"))
                    data['region'].append(region_name)
                    data['region_code'].append(get_region_code(region_name))
                    data['district_code'].append(get_district_code(city))
                else:
                    print(f"Skipping block with insufficient data: {ls}")
        except Exception as e:
            print(f"Error processing city {city}: {e}")
            continue

    new_df = pd.DataFrame(data)
    if new_df.empty:
        print("Warning: No data was scraped. Check website structure or selectors.")
        driver.quit()
        exit()

    print("Unique cities, regions, region codes, and district codes:")
    print(new_df[['city', 'region', 'region_code', 'district_code']].drop_duplicates())

    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path)
    else:
        old_df = pd.DataFrame(columns=new_df.columns)

    merged_df = old_df.copy()
    for _, row in new_df.iterrows():
        mask = (
                (merged_df['city'] == row['city']) &
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
    print(f"Data saved to {file_path}")

finally:
    driver.quit()
