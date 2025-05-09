import os

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By

# Excel fayli nomi
file_path = "forecast.xlsx"

# Brauzerni ishga tushurish
driver = webdriver.Chrome()
driver.get("https://hydromet.uz/")

# Bo'sh dictionary
data = {
    "city": [],
    "day": [],
    "day_part": [],
    "degree": []
}

# Saytdagi barcha shaharlardagi ob-havo ma’lumotlarini olish
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

# Region nomlarini mapping qilish
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

# Yangi ma’lumotlar DataFrame shaklida
new_df = pd.DataFrame(data)
new_df['region'] = [region(x) for x in new_df['city']]

# Eski ma’lumotni o‘qish (agar mavjud bo‘lsa)
if os.path.exists(file_path):
    old_df = pd.read_excel(file_path)
else:
    old_df = pd.DataFrame(columns=new_df.columns)

# Eski va yangi ma’lumotlarni birlashtirishdan oldin: har bir unikal kombinatsiyani tekshirish
merged_df = old_df.copy()

for _, row in new_df.iterrows():
    mask = (
            (merged_df['city'] == row['city']) &
            (merged_df['day'] == row['day']) &
            (merged_df['day_part'] == row['day_part'])
    )

    if mask.any():
        # Agar mavjud bo‘lsa, yangilash (agar degree o‘zgargan bo‘lsa)
        if merged_df.loc[mask, 'degree'].values[0] != row['degree']:
            merged_df.loc[mask, 'degree'] = row['degree']
    else:
        # Agar mavjud bo‘lmasa, yangi qatorda qo‘shish
        merged_df = pd.concat([merged_df, pd.DataFrame([row])], ignore_index=True)

# Faylga saqlash
merged_df.to_excel(file_path, index=False)
driver.quit()
