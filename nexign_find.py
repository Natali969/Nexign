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
    
    # XPath для поиска всех элементов с текстом "Nexign"
    # поиск производится внутри body,
    # так как нужен именно контент страницы,
    # а не её настройки
    xpath = "//body//*[contains(text(), 'Nexign')]"
    # Находим все элементы
    elements_nexign = browser.find_elements(By.XPATH, xpath)
    # выводим количество элементов
    print("Количество упоминаний слова \"Nexign\":", len(elements_nexign))
    # Ждем некоторое время (в данном случае, 5 секунд) перед закрытием браузера
    time.sleep(5)
except Exception as e:
    print(f"Произошла ошибка: {e}")
finally:
    # Закрытие браузера
    browser.quit()