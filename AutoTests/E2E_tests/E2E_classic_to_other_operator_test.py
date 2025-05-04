import pytest
import psycopg2
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium. webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By

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

# Раскладывает CDR-файл на записи, которые записывваются в список
def read_and_parse_file(filepath):
    try:
        with open(filepath, 'r') as file:
            lines = file.readlines()
    except FileNotFoundError:
        print(f"Ошибка: файл '{filepath}' не найден.")
        return []
    data = []
    for line in lines:
        line = line.strip()  # удаляем пробелы в начале и в конце
        if line: # пропускаем пустые строки
            elements = line.split(',')
            data.append(elements)
    return data

# подключение к БД postgres BRT
@pytest.fixture(scope="module")
def db_connection_brt():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="brt-db",
            user="postgres",
            password="postgres",
            port="5432")
        yield conn
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        if conn:
            conn.close()

# подключение к БД postgres HRS
@pytest.fixture(scope="module")
def db_connection_hrs():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="hrs-db",
            user="postgres",
            password="postgres",
            port="5433")
        yield conn
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        if conn:
            conn.close()

# хронология дат в списке соблюдена или нет
def dates_not_increasing(dates):
  """Проверяет, не идут ли даты строго по возрастанию."""
  for i in range(1, len(dates)):
    if dates[i] >= dates[i-1]:
      return True  # Даты идут по возрастанию или есть равные даты
  return False  # Даты не идут строго по возрастанию

def test_add_cdrs(browser, db_connection_brt):
    filepath = 'test_e2e/classic_to_other_operator.txt'
    # раскладываем текстовый файл на массивы
    calls_parameters = read_and_parse_file(filepath)

    # количество записей в БРТ до добавления тестовых
    start_amount_calls = 0
    # количество записей в БРТ после добавления тестовых
    end_amount_calls = 0

    try:
        cur = db_connection_brt.cursor()
        cur.execute("SELECT * FROM cdrs;")
        cdrs_brt = [row for row in cur.fetchall()]
        start_amount_calls = len(cdrs_brt)
        cur.execute(f"SELECT balance FROM subscribers WHERE msisdn = '{calls_parameters[0][1]}' LIMIT 1;")
        for row in cur.fetchall():
            start_balance_A1 = float(row[0])
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")

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
    
    # для каждого из тестируемых звонков
    # вводим запрос на добавление этого звонка в БД
    for i in range(len(calls_parameters)):
        # подставляем данные из файла в запрос для добавления в локальную БД H2
        request = "INSERT INTO CDRS (CALL_TYPE, SERVICED_MSISDN, OTHER_MSISDN, START_DATE_TIME,"
        request += f"FINISH_DATE_TIME, CONSUMED_STATUS) VALUES ('{calls_parameters[i][0]}', "
        request += f"'{calls_parameters[i][1]}', '{calls_parameters[i][2]}',"
        request += f" '{calls_parameters[i][3]}', '{calls_parameters[i][4]}', 'NEW');"
        try:
            # в консоли вводим запрос на добавление записей в бд
            input_request = browser.find_element(By.XPATH, "//textarea[@name='sql']")
            input_request.clear()
            input_request.send_keys(request)
            button_request = browser.find_element(By.CSS_SELECTOR, "input[value='Run']")
            button_request.click()
        except Exception as e:
            pytest.fail(f"Ошибка при взаимодействии с H2: {e}")
    # ждём, пока записи из H2 попадут в БРТ
    time.sleep(10)

    try:
        # проверяем, сколько записей стало посл добавления тестовых
        cur = db_connection_brt.cursor()
        cur.execute("SELECT * FROM cdrs;")
        cdrs_brt = [row for row in cur.fetchall()]
        end_amount_calls = len(cdrs_brt)
        cur.execute(f"SELECT balance FROM subscribers WHERE msisdn = '{calls_parameters[0][1]}' LIMIT 1;")
        for row in cur.fetchall():
            end_balance_A1 = float(row[0])
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    
    # должны добавиться все записи из файла, кроме одной (9 штук)
    # десятая содержит звонок абонента с некорректной датой
    assert end_amount_calls - start_amount_calls == 10
    # баланс первого абонента уменьшился на нужную сумму
    assert start_balance_A1 - end_balance_A1 == 2.5