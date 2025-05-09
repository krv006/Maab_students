import os

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By

file_path = "forecast.xlsx"

driver = webdriver.Chrome()
driver.get("https://hydromet.uz/")

data = {
    "city": [],
    "day": [],
    "day_part": [],
    "degree": []
}

options = driver.find_elements(By.CSS_SELECTOR, "select option")
for option in options:
    try:
        city = option.text
        option.click()
        blocks = driver.find_elements(By.CSS_SELECTOR, ".weather-widget__forecast__table .day")
        for block in blocks:
            ls = block.text.split("\n")
            data['city'].append(city)
            data['day'].append(ls[0])
            data["day_part"].append(ls[1])
            data['degree'].append(ls[2].replace("…", "-"))
    except:
        continue

city_region = {
    "tashkent": "Tashkent",
    "gulistan": "Sirdaryo viloyati",
    "urgench": "Xorazm",
    "nukus": "Qoraqolpog'iston respublikasi",
    "bukhara": "Bukhara",
    "navoiy": "Navoiy",
    "samarkand": "Samarkand",
    "jizzakh": "Jizzakh",
    "qarshi": "Qashqadaryo",
    "termez": "Surkhandarya",
    "fergana": "Fergana",
    "namangan": "Namangan",
    "andijan": "Andijan",
    "nurafshon": "Tashkent viloyati",
    "kamchik": "Andijon",
    "chimgan": "Tashkent"
}
region = lambda x: city_region.get(x.lower(), "Unknown").capitalize()

new_df = pd.DataFrame(data)
new_df['region'] = [region(x) for x in new_df['city']]

if os.path.exists(file_path):
    old_df = pd.read_excel(file_path)
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
    else:
        merged_df = pd.concat([merged_df, pd.DataFrame([row])], ignore_index=True)

merged_df.to_excel(file_path, index=False)
driver.quit()
