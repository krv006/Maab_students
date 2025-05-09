import os

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By

file_path = "kamron.xlsx"


# Define the region code mapping function
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
        # City and simplified region mappings
        "Tashkent": "1727",  # Treat Tashkent distirct as Toshkent viloyati
        "Andijan": "1703",
        "Bukhara": "1706",
        "Samarkand": "1718",
        "Qarshi": "1710",
        "Fergana": "1730",
        "Namangan": "1714",
        "Sirdaryo": "1724",
        "Qoraqolpog'iston respublikasi": "1735",
        "Surkhandarya": "1722",
        "Navoiy": "1712",
        "Jizzakh": "1708",
        "Qashqadaryo": "1710",
        "Xorazm": "1733",
    }
    return region_codes.get(region_name, "Unknown")


# Initialize WebDriver
driver = webdriver.Chrome()
driver.get("https://hydromet.uz/")

data = {
    "distirct": [],
    "day": [],
    "day_part": [],
    "degree": []
}

# Scrape weather data
options = driver.find_elements(By.CSS_SELECTOR, "select option")
for option in options:
    try:
        distirct = option.text
        option.click()
        blocks = driver.find_elements(By.CSS_SELECTOR, ".weather-widget__forecast__table .day")
        for block in blocks:
            ls = block.text.split("\n")
            data['distirct'].append(distirct)
            data['day'].append(ls[0])
            data["day_part"].append(ls[1])
            data['degree'].append(ls[2].replace("…", "-"))
    except:
        continue

# Define distirct to region mapping
distirct_region = {
    "tashkent": "Toshkent viloyati",
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
    "nurafshon": "Toshkent viloyati",
    "kamchik": "Andijan",
    "chimgan": "Toshkent viloyati"
}
region = lambda x: distirct_region.get(x.lower(), "Unknown")

# Create new DataFrame with region and region_code
new_df = pd.DataFrame(data)
new_df['region'] = [region(x) for x in new_df['distirct']]
new_df['region_code'] = [get_region_code(region(x)) for x in new_df['distirct']]

# Debug: Print unique regions and their codes to check mappings
print("Unique regions and codes:")
print(new_df[['region', 'region_code']].drop_duplicates())

# Load existing data or create empty DataFrame
if os.path.exists(file_path):
    old_df = pd.read_excel(file_path)
else:
    old_df = pd.DataFrame(columns=new_df.columns)

# Merge new data with old data
merged_df = old_df.copy()

for _, row in new_df.iterrows():
    mask = (
            (merged_df['distirct'] == row['distirct']) &
            (merged_df['day'] == row['day']) &
            (merged_df['day_part'] == row['day_part'])
    )

    if mask.any():
        if merged_df.loc[mask, 'degree'].values[0] != row['degree']:
            merged_df.loc[mask, 'degree'] = row['degree']
    else:
        merged_df = pd.concat([merged_df, pd.DataFrame([row])], ignore_index=True)

# Save to Excel
merged_df.to_excel(file_path, index=False)

# Close WebDriver
driver.quit()


"NURAFSHON"
"NURAFSHON"

"""
andijon bn qamchiq region code bir xil 
"""