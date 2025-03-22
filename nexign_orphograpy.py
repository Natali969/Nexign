from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
# Импортируется необходимый класс By из библиотеки Selenium
from selenium.webdriver.common.by import By
import requests

def speller(text: str) -> str:
    frase_for_speller = text.replace(' ', '+')
    url = f'https://speller.yandex.net/services/spellservice.json/checkText?text={frase_for_speller}'
    request = requests.get(
        url=url
    )
    if request.text == 'Request uri too large':
        return request.text
    data = request.json()
    # if len(data) == 0:
    #     return 0
    word_list = []
    for word in data:
        word_list.append(word.get('s')[0])

    final_frase = ' '.join(word_list)
    return final_frase
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

    # список всех проверяемых ссылок со страницы
    all_links = []
    # Найдём все возможные ссылки на главной странице
    # при поиске по тегу а возникали лишние ссылки на сторонние ресурсы,
    # например, https://vc.ru/nexign, почту и т.д.
    # при этом нам нужны ссылки, такие, как https://neonhrm.nexign.com/
    # https://job.nexign.com/ и т.п.
    all_a = browser.find_elements(By.XPATH, """//a[starts-with(@href, '/ru') or
                                  contains (@href, 'https://nexign') or
                                  (starts-with(@href, 'https://') and contains (@href, 'nexign.'))]""")
    # заполняем список найденными ссылками
    for elem in all_a:
        all_links.append(elem.get_attribute("href"))
    print(all_links[0])
    pages_text = []
    check_results = []
    for i,link in enumerate(all_links[:6]):
        # для проверки только текста первых пяти страниц и главной
        browser.get(link)
        page_source = browser.page_source
        element = browser.find_element(By.TAG_NAME, "body")
        pages_text.append(element.text)
        # название компании без кавычек сервисом считается
        # за ошибку, поэтому это слово убирается из проверок
        pages_text[i] = pages_text[i].replace("Nexign", "")
        check_results.append(len(speller(pages_text[i])))

    # Ждем некоторое время (в данном случае, 5 секунд) перед закрытием браузера
    # time.sleep(5)
except Exception as e:
    print(f"Произошла ошибка: {e}")
finally:
    # Закрытие браузера
    browser.quit()
    assert sum(check_results) == 0