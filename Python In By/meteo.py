from selenium import webdriver
from selenium.webdriver.common.by import By
import time

driver = webdriver.Chrome()
driver.get("https://data.meteo.uz/")
time.sleep(5)  # Sahifa yuklanishini kutamiz

# Scroll qilish orqali barcha shaharlardagi ma’lumotlarni yuklaymiz
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(3)

data2 = {
    "city": [],
    "day": [],
    "day_part": [],
    "degree": []
}

cards = driver.find_elements(By.CSS_SELECTOR, ".forecast-card")
for card in cards:
    try:
        city = card.find_element(By.CSS_SELECTOR, ".card-city-name").text
        items = card.find_elements(By.CSS_SELECTOR, ".forecast-day-item")
        for item in items:
            day = item.find_element(By.CSS_SELECTOR, ".forecast-day-title").text.strip()
            parts = item.find_elements(By.CSS_SELECTOR, ".forecast-part")
            for part in parts:
                day_part = part.find_element(By.CSS_SELECTOR, ".forecast-part-title").text.strip()
                temp = part.find_element(By.CSS_SELECTOR, ".forecast-temp").text.strip().replace("…", "-")

                data2["city"].append(city)
                data2["day"].append(day)
                data2["day_part"].append(day_part)
                data2["degree"].append(temp)
    except Exception as e:
        print("Xatolik:", e)
        continue
