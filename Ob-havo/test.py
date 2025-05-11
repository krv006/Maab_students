from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import csv
import pandas as pd
import datetime
import logging
import os
import pyodbc

# Logging sozlamalari
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Region kodlarini aniqlash funksiyasi
def get_region_code(title):
    match title:
        case "MeteoBot-36 / MeteoUz": return None
        case "ЯНГИКЎРҒОН": return 1714
        case "Ходжейли": return 1735
        case "Раушан": return None
        case "Канлыкуль": return 1735
        case "Шoманай": return 1735
        case "Сары-алтын": return None
        case "Уллыбаг": return None
        case "Шурахан": return None
        case "Мангит": return 1735
        case "Фаргона шахри": return 1730
        case "Бешарык": return 1730
        case "Дангара": return 1730
        case "Хоразм Кошкупыр": return 1733
        case "Лебап": return None
        case "Хива, Дашяк": return 1733
        case "Тошкент обсерватория": return 1727
        case "Ургенч шахар": return 1733
        case "Хоразм Гурлен": return 1733
        case "Самарканд шахри": return 1718
        case "Самарканд Фарход Зарафшон": return 1718
        case "Самарканд Бyлyнгyp": return 1718
        case "Самарканд Ургут": return 1718
        case "Жиззах Мирзачўл тумани": return 1708
        case "Самарканд Янгиарик": return 1718
        case "Жиззах Баҳмал Муғол": return 1708
        case "Жиззах Янгиобод Хўжамушкент": return 1708
        case "Жиззах Бахмал": return 1708
        case "Жиззах Пахтакор": return 1708
        case "Сирдарё Сардоба": return 1724
        case "Сирдарё Околтин": return 1724
        case "Сирдарё Боёвут": return 1724
        case "Сирдарё Гулистон ш.": return 1724
        case "Сирдарё Сайхунобод": return 1724
        case "Андижон Боз": return 1703
        case "Андижон Хонабод": return 1703
        case "Андижон УГМ": return 1703
        case "Андижон Коргонтепа": return 1703
        case "Андижон Улугнор": return 1703
        case "Surxondaryo, Boysun": return 1722
        case "Surxondaryo, Jarqo'rg'on, G'ur-G'ur": return 1722
        case "Surxondaryo, Termiz tumani, Navruz": return 1722
        case "Surxondaryo, Qumqo'rg'on tumani, To'g'on": return 1722
        case "Qashqadaryo, Koson tumani, Pulat shaharcha": return 1710
        case "Qashqadaryo, Qarshi shahar, Chiroqchi": return 1710
        case "Qashqadaryo, Qamashi": return 1710
        case "Surxondaryo, Oltinsoy": return 1722
        case "Qashqadaryo, Mirishkor tumani, Naiston": return 1710
        case "Qashqadaryo, Nishon tumani": return 1710
        case "Зангиота, Бузсув": return 1727
        case "Тошкент вилояти, Куйи чирчик": return 1727
        case "Тошкент вилояти, Ўрта чирчиқ": return 1727
        case "Yuqori Chirchiq tumani": return 1727
        case "Qibray tumani": return 1727
        case "Bo'stonliq tumani": return 1727
        case "Бухара": return 1706
        case "Buxoro viloyati, G'alla osiyo": return 1706
        case "Buxoro shahar, Jondor tuman, Yakkatut AMP": return 1706
        case "Жиззах вилояти, Зомин, Суффа Платоси": return 1708
        case "Toshkent shahar, Olmazor tumani": return 1727
        case "Toshkent,Yangi O'zbekiston bog'i": return 1727
        case "М-II Дехканабад": return 1710
        case "М-II Муборек": return 1710
        case "О Чимкурган": return None
        case "Г-1 Шахрисябз": return 1710
        case "М-II Гузар": return 1710
        case "106_Taxtakaracha": return None
        case "М-II Бахмал": return 1708
        case "АГМС Галляарал": return 1708
        case "М-II Дустлик": return 1708
        case "Ляльмикор": return None
        case "111_Zomin": return 1708
        case "М-II Янгикишлак": return None
        case _: return None

def scrape_meteo_data():
    """Scrape agrometeorological data and append to JSON, CSV, and directly to SQL Server."""
    # Start browser
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=options)
    driver.get("https://data.meteo.uz/")
    wait = WebDriverWait(driver, 15)

    # Timestamp va sana
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")

    # Fayl nomlari
    json_filename = "meteo_data.json"
    csv_filename = "meteo_data.csv"

    # SQL Serverga ulanish
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=192.168.111.14;"
            "DATABASE=weather;"
            "UID=sa;"
            "PWD=AX8wFfMQrR6b9qdhHt2eYS;"
        )
        cursor = conn.cursor()
        logger.info("SQL Serverga ulanish muvaffaqiyatli.")
    except Exception as e:
        logger.error(f"SQL Serverga ulanishda xatolik: {e}")
        driver.quit()
        raise

    try:
        # Step 1: Dropdown menyuni ochish
        dropdown_toggle = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "calcite-dropdown-toggle")))
        dropdown_toggle.click()
        logger.info("Dropdown menu clicked.")

        # Step 2: 'Информация' ni tanlash
        info_item = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//ul[contains(@class, 'dropdown-menu')]/li/a[@data-target='#panelMeteodata']")
        ))
        info_item.click()
        logger.info("'Информация' menu clicked.")

        # Step 3: 'Агрометеорологичкские данные' ni tanlash
        select_element = wait.until(EC.presence_of_element_located((By.ID, "selectStandardMeteodata")))
        select = Select(select_element)
        select.select_by_value("meteo_agro")
        logger.info("'Агрометеорологичкские данные' selected.")

        # Step 4: Xarita yuklanishini kutish
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "leaflet-map-pane")))
        logger.info("Map loaded.")

        # Step 5: Navigatsiya panelini yashirish
        try:
            driver.execute_script("""
                var nav = document.querySelector('.calcite-navbar');
                if (nav) nav.style.display = 'none';
            """)
            logger.info("Navigation bar hidden.")
        except:
            logger.warning("Could not hide navigation bar, continuing.")

        # Step 6: Barcha markerlarni topish
        markers = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "leaflet-marker-icon")))
        logger.info(f"Found {len(markers)} markers.")

        # Ma'lumotlarni saqlash uchun ro‘yxat
        all_data = []
        inserted_rows = 0
        failed_rows = []

        # SQL so‘rovi
        sql_query = """
            INSERT INTO meteo_data_test (
                date,
                region_code,
                timestamp,
                title,
                humidity_10cm,
                humidity_20cm,
                humidity_30cm,
                date_time,
                temp_10cm,
                temp_20cm,
                temp_30cm
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for index, marker in enumerate(markers):
            max_attempts = 3
            attempt = 1
            while attempt <= max_attempts:
                try:
                    # Marker ni ko‘rinadigan qilish
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", marker)
                    time.sleep(0.5)

                    # Marker ni bosish
                    driver.execute_script("arguments[0].click();", marker)
                    logger.info(f"Clicked marker {index + 1} (attempt {attempt}).")

                    # Popup ni kutish
                    popup = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "leaflet-popup-content")))
                    logger.info(f"Popup appeared for marker {index + 1}.")

                    # Jadval ma'lumotlarini olish
                    table = popup.find_element(By.CLASS_NAME, "table")
                    rows = table.find_elements(By.TAG_NAME, "tr")
                    data = {"Date": date_str, "Timestamp": timestamp}
                    for row in rows:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) == 2:
                            key = cells[0].text.strip()
                            value = cells[1].text.strip()
                            data[key] = value
                        elif len(cells) == 1:
                            data["Title"] = cells[0].text.strip()

                    # Region kodini qo‘shish
                    data["Region Code"] = get_region_code(data.get("Title", ""))

                    # Ma'lumotlarni ro‘yxatga qo‘shish (CSV va JSON uchun)
                    all_data.append(data)
                    logger.info(f"Data extracted for marker {index + 1}: {data}")

                    # Ma'lumotlarni to‘g‘ridan-to‘g‘ri SQL Serverga kiritish
                    try:
                        cursor.execute(sql_query, (
                            data.get("Date"),
                            data.get("Region Code"),
                            data.get("Timestamp"),
                            data.get("Title"),
                            data.get("Влажность почвы 10 см (-10)"),
                            data.get("Влажность почвы 20 см (-20)"),
                            data.get("Влажность почвы 30 см (-30)"),
                            data.get("Дата"),
                            data.get("Температура почвы 10 см (-10)"),
                            data.get("Температура почвы 20 см (-20)"),
                            data.get("Температура почвы 30 см (-30)")
                        ))
                        inserted_rows += 1
                        logger.info(f"Marker {index + 1} ma'lumotlari SQL Serverga kiritildi.")
                    except Exception as e:
                        failed_rows.append((index, data, str(e)))
                        logger.error(f"Marker {index + 1} ni SQL Serverga kiritishda xatolik: {e}")

                    # Popup ni yopish
                    try:
                        close_button = driver.find_element(By.CLASS_NAME, "leaflet-popup-close-button")
                        driver.execute_script("arguments[0].click();", close_button)
                        time.sleep(0.5)
                        logger.info(f"Popup closed for marker {index + 1}.")
                    except:
                        logger.warning(f"No close button found for marker {index + 1}, continuing.")

                    break

                except Exception as e:
                    logger.warning(f"Error processing marker {index + 1} (attempt {attempt}): {e}")
                    attempt += 1
                    if attempt > max_attempts:
                        logger.error(f"Failed to process marker {index + 1} after {max_attempts} attempts.")
                        break
                    time.sleep(1)

        # Step 7: JSON ga qo‘shish
        existing_data = []
        if os.path.exists(json_filename):
            try:
                with open(json_filename, 'r', encoding='utf-8') as json_file:
                    existing_data = json.load(json_file)
            except Exception as e:
                logger.warning(f"Error reading existing JSON file: {e}")

        existing_data.extend(all_data)
        with open(json_filename, 'w', encoding='utf-8') as json_file:
            json.dump(existing_data, json_file, ensure_ascii=False, indent=4)
        logger.info(f"Data appended to {json_filename}")

        # Step 8: CSV ga qo‘shish
        if all_data:
            df = pd.DataFrame(all_data)
            headers = sorted(set().union(*(d.keys() for d in all_data)))
            mode = 'a' if os.path.exists(csv_filename) else 'w'
            with open(csv_filename, mode, encoding="utf-8", newline="") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=headers)
                if mode == 'w':
                    writer.writeheader()
                for data in all_data:
                    writer.writerow(data)
            logger.info(f"Data appended to {csv_filename}")

        # Step 9: SQL Server xatolarini log qilish
        if failed_rows:
            with open('failed_sql_rows.txt', 'w', encoding='utf-8') as f:
                for index, data, error in failed_rows:
                    f.write(f"Marker {index + 1}: {data}\nXatolik: {error}\n\n")
            logger.error(f"SQL Serverga kiritishda xatolik yuz bergan qatorlar soni: {len(failed_rows)}. Batafsil: failed_sql_rows.txt")

        # Step 10: SQL o‘zgarishlarini saqlash
        conn.commit()
        logger.info(f"Muvaffaqiyatli kiritilgan qatorlar soni (SQL Server): {inserted_rows}")

        # Ma'lumotlarni chop etish
        logger.info("\nAll extracted data:")
        for i, data in enumerate(all_data, 1):
            logger.info(f"\nMarker {i}:")
            for key, value in data.items():
                logger.info(f"  {key}: {value}")

    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        raise

    finally:
        # SQL ulanishni yopish
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        logger.info("SQL Server connection closed.")
        # Brauzerni yopish
        time.sleep(2)
        driver.quit()
        logger.info("Browser closed.")

if __name__ == "__main__":
    logger.info("Starting scrape...")
    scrape_meteo_data()
    logger.info("Scrape completed successfully.")