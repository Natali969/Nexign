from selenium import webdriver
import time
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
# Импортируется необходимый класс By из библиотеки Selenium
from selenium.webdriver.common.by import By
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

try:
    # Открытие веб-системы
    browser.get('https://nexign.com/ru')
    xpath = "//li[@class='menu-item menu-item--expanded']//span[contains(., 'Продукты и решения')]"
    # ищем раздел 'Продукты и решения'
    button_products = browser.find_element(By.XPATH, xpath)
    # Нажатие на кнопку с этим разделом
    button_products.click()
    # ищем раздел "Инструменты для ИТ-команд"
    button_instruments = browser.find_element(By.CSS_SELECTOR, 'ul > li.menu-item.menu-item--expanded.active > ul > li:nth-child(5)')
    button_instruments.click()
    # для поиска раздела Nexign Nord
    # на странице 2 ссылки на него,
    # поэтому ищем первый из них
    xpath = "//a[1][@data-drupal-link-system-path='node/1353']"
    # Находим раздел
    button = browser.find_element(By.XPATH, xpath)
    # Нажатие на кнопку с разделом
    button.click()
    # Ждем некоторое время (в данном случае, 5 секунд) перед закрытием браузера
    time.sleep(5)
except Exception as e:
    print(f"Произошла ошибка: {e}")
finally:
    # Закрытие браузера
    browser.quit()