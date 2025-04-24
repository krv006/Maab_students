from playwright.sync_api import sync_playwright
import pandas as pd
import logging
import sys

# Logging sozlash
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def scrape_weather():
    forecast = []

    with sync_playwright() as p:
        try:
            # Brauzerni ishga tushirish (headless rejimda)
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            logging.info("Brauzer ishga tushirildi")

            # Sahifani ochish
            page.goto(
                "https://weather.com/weather/tenday/l/7a00b0e83f04765af07b14cc1fe2e7a3095921086ff5df1c7ce05a86d123229f")
            logging.info("Sahifa yuklanmoqda...")

            # Sahifaning to'liq yuklanishini kutish
            page.wait_for_selector('details.summary-daily', timeout=15000)
            logging.info("Sahifa muvaffaqiyatli yuklandi")

            # Ma'lumotlarni yig'ish
            rows = page.query_selector_all('details.summary-daily')

            for index, row in enumerate(rows, start=1):
                try:
                    day = row.query_selector('h3.DailyForecast--daypartName--ImZZL').inner_text()
                    date = row.query_selector('span.DailyForecast--date--3r1aJ').inner_text()
                    desc = row.query_selector('p.DailyForecast--narrative--WwhlL').inner_text()
                    temp_high = row.query_selector('span.DailyForecast--highTempValue--1Xl83').inner_text()
                    temp_low = row.query_selector('span.DailyForecast--lowTempValue--3RwN2').inner_text()
                    rain = row.query_selector('div.DailyForecast--precip--1gV0e').inner_text()

                    forecast.append({
                        'Kun': day,
                        'Sana': date,
                        'Tavsif': desc,
                        'Yuqori harorat': temp_high,
                        'Past harorat': temp_low,
                        'Yomg‘ir ehtimoli': rain
                    })
                    logging.info(f"{index}-kun ma'lumotlari yig'ildi: {day}")

                except Exception as e:
                    logging.warning(f"{index}-kun ma'lumotlarini olishda xato: {e}")
                    continue

            # Brauzerni yopish
            browser.close()
            logging.info("Brauzer yopildi")

        except Exception as e:
            logging.error(f"Umumiy xato: {e}")
            sys.exit(1)

    return forecast


# Ma'lumotlarni yig'ish va CSV faylga yozish
forecast = scrape_weather()
if forecast:
    df = pd.DataFrame(forecast)
    df.to_csv("weather_10_days.csv", index=False, encoding='utf-8')
    logging.info("Ma'lumotlar 'weather_10_days.csv' fayliga saqlandi")
else:
    logging.error("Hech qanday ma'lumot yig'ilmadi")