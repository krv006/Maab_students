from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd

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
        break

city_region = {
    "tashkent": "Tashkent",
    "gulistan": "Sirdaryo viloyati",
    "urgench": "Xorazm",
    "nukus": "Qoraqolpog'iston respublikasi",
    "bukhara": "Bukhara",
    "navoiy": "Navoiy",
    "samarkand": "samarkand",
    "jizzakh": "jizzakh",
    "qarshi": "qashqadaryo",
    "termez": "surkhandarya",
    "fergana": "fergana",
    "namangan": "namangan",
    "andijan": "andijan",
    "nurafshon": "tashkent viloyati",
    "kamchik": "andijon",
    "chimgan": "tashkent"
}

region = lambda x: city_region[x].capitalize()

df = pd.DataFrame(data)

df['region'] = [region(x.lower()) for x in df['city']]

df.to_excel("forecast.xlsx", index=False)
driver.quit()
