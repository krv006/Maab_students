from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd

# Web driver'ni ishga tushurish
driver = webdriver.Chrome()
driver.get("https://hydromet.uz/")

# Malumotlar saqlash uchun bo'sh lug'at yaratish
data = {
    "city": [],
    "day": [],
    "day_part": [],
    "degree": []
}

# Shaharlar ro'yxatiga o'tish
for i in range(1, 17):
    option = driver.find_element(By.XPATH, f"/html/body/div[2]/div[1]/header/div[2]/div/select/option[{i}]")
    option.click()
    city = option.text
    for i in range(1, 7):
        day = driver.find_element(By.XPATH, f"/html/body/div[2]/div[1]/header/div[2]/div/div[4]/div/div[{i}]/h4").text
        day_part = driver.find_element(By.XPATH, f"/html/body/div[2]/div[1]/header/div[2]/div/div[4]/div/div[{i}]//span").text
        degree = driver.find_element(By.XPATH, "//span[@class='temp']").text
        data['city'].append(option.text)
        data['day'].append(day)
        data["day_part"].append(day_part)
        data['degree'].append(degree)

df = pd.DataFrame(data)

df.to_excel("forecast.xlsx", index=False)

driver.quit()
