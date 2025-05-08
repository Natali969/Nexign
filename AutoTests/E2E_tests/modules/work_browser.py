from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium. webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
import pytest
import time

@pytest.fixture(scope="module")
def browser():
    # Путь к исполняемому файлу chromedriver.exe
    chrome_driver_path = 'chromedriver-win64/chromedriver.exe'
    # Путь к исполняемому файлу Яндекс.Браузера (нужно указать актуальный путь)
    yandex_browser_path = r'C:\Users\user\AppData\Local\Yandex\YandexBrowser\Application\browser.exe' 
    # Создание сервиса Chrome
    chrome_service = ChromeService(executable_path=chrome_driver_path)
    # Создание экземпляра ChromeOptions и указание пути к Яндекс.Браузеру
    chrome_options = ChromeOptions()
    chrome_options.binary_location = yandex_browser_path
    # Создание экземпляра браузера с использованием ChromeOptions
    browser = webdriver.Chrome(service=chrome_service, options=chrome_options)
    yield browser
    browser.quit()

def open_h2_console(browser):
     # откроем браузер для доступа к консоли h2
    try:
        # Открытие веб-системы
        browser.get('http://localhost:8082/h2-console')
        time.sleep(1)
        # ввод данных для авторизации
        input1 = browser.find_element(By.XPATH, "//input[@name='driver']")
        input1.clear()
        input1.send_keys("org.h2.Driver")
        input2 = browser.find_element(By.XPATH, "//input[@name='url']")
        input2.clear()
        input2.send_keys("jdbc:h2:mem:cdr-db")
        input3 = browser.find_element(By.XPATH, "//input[@name='user']")
        input3.clear()
        input3.send_keys("sa")
        input4 = browser.find_element(By.XPATH, "//input[@name='password']")
        input4.clear()
        input4.send_keys("password")
        # войти в консоль управления БД
        button = browser.find_element(By.CSS_SELECTOR, 'input[type="submit"]')
        button.click()

        # чтобы искать элементы,
        # нужно переключиться на нужный фреймсет
        # в виду таковой разметки страницы (устаревшая)
        menu_frame = browser.find_element(By.NAME, "h2query")
        browser.switch_to.frame(menu_frame)
    except Exception as e:
        pytest.fail(f"Ошибка входа в консоль H2: {e}")