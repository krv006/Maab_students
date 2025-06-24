import time
import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
import logging
import sys
from typing import Optional

# Logging sozlamalari
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Konstantalar
EMAIL = "powerbi@epco.com"
PASSWORD = "said_2021"
LOGIN_URL = "https://smartup.online"
DASHBOARD_URL = "https://smartup.online/#/!44lnbqonn/trade/intro/dashboard"
DATA_URL = "https://smartup.online/b/anor/mxsx/mdeal/return$export"
OUTPUT_FILE = "smartup_return_export.json"


def setup_chrome_driver() -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    try:
        driver = webdriver.Chrome(options=chrome_options)
        logger.info("✅ Chrome driver muvaffaqiyatli sozlandi.")
        return driver
    except WebDriverException as e:
        logger.error(f"❌ Chrome driver sozlashda xatolik: {e}")
        sys.exit(1)


def find_element(driver: webdriver.Chrome, by: By, value: str, timeout: int = 40) -> Optional[
    webdriver.remote.webelement.WebElement]:
    """Elementni topish uchun moslashuvchan funksiya."""
    try:
        return WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
    except TimeoutException:
        logger.error(f"❌ Element topilmadi: {by} = {value}")
        return None


def get_cookies_with_login(login_url: str, dashboard_url: str, email: str, password: str) -> dict:
    logger.info("🔐 Login jarayoni boshlandi...")

    driver = setup_chrome_driver()
    try:
        logger.info(f"🌐 Sahifa ochilmoqda: {login_url}")
        driver.get(login_url)

        logger.info("⌛ Sahifa to‘liq yuklanishi kutilmoqda...")
        time.sleep(10)

        # HTML tuzilishini saqlash (debug uchun)
        try:
            with open("login_page.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.info("📜 Login sahifasi HTML tuzilishi 'login_page.html' fayliga saqlandi")
        except Exception as e:
            logger.error(f"❌ HTML faylni saqlashda xatolik: {e}")

        # Email maydonini topish
        logger.info("⌛ Email maydoni kutilmoqda...")
        email_field = find_element(driver, By.NAME, "email")
        if not email_field:
            email_field = find_element(driver, By.ID, "email") or \
                          find_element(driver, By.ID, "user_email") or \
                          find_element(driver, By.CSS_SELECTOR, "input[type='email']") or \
                          find_element(driver, By.XPATH,
                                       "//input[contains(@placeholder, 'Email') or contains(@placeholder, 'email') or contains(@placeholder, 'E-mail')]") or \
                          find_element(driver, By.XPATH,
                                       "//input[contains(@name, 'email') or contains(@name, 'Email')]") or \
                          find_element(driver, By.XPATH, "//input[contains(@id, 'email') or contains(@id, 'Email')]")
        if not email_field:
            raise NoSuchElementException("Email maydoni topilmadi")
        email_field.clear()
        email_field.send_keys(email)
        logger.info("✅ Email kiritildi.")

        # Parol maydonini topish
        logger.info("⌛ Parol maydoni kutilmoqda...")
        password_field = find_element(driver, By.NAME, "password")
        if not password_field:
            password_field = find_element(driver, By.ID, "password") or \
                             find_element(driver, By.ID, "user_password") or \
                             find_element(driver, By.CSS_SELECTOR, "input[type='password']") or \
                             find_element(driver, By.XPATH,
                                          "//input[contains(@placeholder, 'Password') or contains(@placeholder, 'password')]") or \
                             find_element(driver, By.XPATH,
                                          "//input[contains(@name, 'password') or contains(@name, 'Password')]") or \
                             find_element(driver, By.XPATH,
                                          "//input[contains(@id, 'password') or contains(@id, 'Password')]")
        if not password_field:
            raise NoSuchElementException("Parol maydoni topilmadi")
        password_field.clear()
        password_field.send_keys(password)
        logger.info("✅ Parol kiritildi.")

        # Login tugmasini topish
        logger.info("⌛ Login tugmasi kutilmoqda...")
        login_button = find_element(driver, By.XPATH, '//button[@type="submit"]')
        if not login_button:
            login_button = find_element(driver, By.CSS_SELECTOR, "button[type='submit'], input[type='submit']") or \
                           find_element(driver, By.XPATH,
                                        "//button[contains(text(), 'Login') or contains(text(), 'Kirish') or contains(text(), 'Submit') or contains(text(), 'Sign in')]") or \
                           find_element(driver, By.CLASS_NAME, "login-btn") or \
                           find_element(driver, By.CLASS_NAME, "btn-login") or \
                           find_element(driver, By.XPATH,
                                        "//button[contains(@class, 'login') or contains(@class, 'submit')]")
        if not login_button:
            raise NoSuchElementException("Login tugmasi topilmadi")
        login_button.click()
        logger.info("✅ Login tugmasi bosildi.")

        # Dashboard sahifasiga o‘tish
        logger.info(f"🌐 Dashboard sahifasiga o‘tilyapti: {dashboard_url}")
        driver.get(dashboard_url)

        # Sahifa yuklanishini kutish
        logger.info("⌛ Dashboard sahifasi yuklanishi kutilmoqda...")
        time.sleep(10)

        # Dashboard HTML tuzilishini saqlash
        try:
            with open("dashboard_page.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.info("📜 Dashboard sahifasi HTML tuzilishi 'dashboard_page.html' fayliga saqlandi")
        except Exception as e:
            logger.error(f"❌ Dashboard HTML faylni saqlashda xatolik: {e}")

        # Cookie'larni olish
        cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
        if not cookies:
            logger.warning("⚠️ Cookie'lar topilmadi. Autentifikatsiyani tekshiring.")
        else:
            logger.info(f"✅ {len(cookies)} ta cookie muvaffaqiyatli olindi.")
        return cookies

    except TimeoutException as e:
        logger.error(f"❌ Vaqt tugashi xatosi: {e}")
        raise RuntimeError(f"Login yoki dashboard jarayonida vaqt tugashi xatosi: {e}")
    except NoSuchElementException as e:
        logger.error(f"❌ Element topilmadi: {e}")
        raise RuntimeError(f"Login sahifasida element topilmadi: {e}")
    except WebDriverException as e:
        logger.error(f"❌ WebDriver xatosi: {e}")
        raise RuntimeError(f"WebDriver xatosi: {e}")
    finally:
        if 'driver' in locals():
            driver.quit()
            logger.info("🧹 Chrome driver yopildi.")


def explore_json(data, prefix: str = "") -> list:
    """JSON ichidagi ro'yxatlarni topadi."""
    keys = []
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                keys.extend(explore_json(value, f"{prefix}{key}."))
    elif isinstance(data, list) and data:
        keys.append(prefix[:-1])
    return keys


def fetch_and_export_data(data_url: str, output_file: str, cookies: dict) -> None:
    """Ma'lumotl Himalaya

System: lumotlarni yuklab, JSON faylga saqlaydi."""
    logger.info("⬇️ Ma'lumotlar yuklanmoqda...")

    try:
        response = requests.get(data_url, cookies=cookies, timeout=30)
        response.raise_for_status()

        logger.info(f"📄 Content-Type: {response.headers.get('Content-Type', '').lower()}")

        data = response.json()
        list_keys = explore_json(data)
        logger.info(f"🔎 Ro'yxat kalitlari: {list_keys if list_keys else 'Topilmadi'}")

        df = None
        if list_keys:
            for key in list_keys:
                try:
                    nested_data = data
                    for part in key.split("."):
                        nested_data = nested_data[part]
                    df = pd.json_normalize(nested_data, errors="ignore")
                    logger.info(f"✅ '{key}' dan DataFrame yaratildi")
                    break
                except (KeyError, TypeError) as e:
                    logger.warning(f"⚠️ '{key}' kalitida xatolik: {e}")
                    continue
        else:
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                raise ValueError("JSON noto‘g‘ri formatda")

        if df is not None and not df.empty:
            df.to_json(output_file, orient="records", indent=4, force_ascii=False)
            logger.info(f"✅ JSON fayl saqlandi: {output_file}")
            logger.info("📊 Namuna:")
            logger.info(df.head(2).to_string())
        else:
            raise ValueError("DataFrame bo‘sh yoki xatolik bor")

    except requests.RequestException as e:
        logger.error(f"❌ HTTP so‘rov xatosi: {e}")
        raise
    except ValueError as e:
        logger.error(f"❌ Ma'lumot ishlov berishda xatolik: {e}")
        with open("smartup_export.txt", "wb") as f:
            f.write(response.content)
        try:
            logger.info("📜 JSON namunasi:")
            logger.info(json.dumps(data, indent=2)[:500])
        except:
            logger.error("📜 JSONni chiqarib bo‘lmadi")
        raise
    except Exception as e:
        logger.error(f"❌ Umumiy xatolik: {e}")
        raise


if __name__ == "__main__":
    try:
        cookies = get_cookies_with_login(LOGIN_URL, DASHBOARD_URL, EMAIL, PASSWORD)
        fetch_and_export_data(DATA_URL, OUTPUT_FILE, cookies)
    except Exception as e:
        logger.error(f"🏁 Dastur xatosi: {e}")
        sys.exit(1)