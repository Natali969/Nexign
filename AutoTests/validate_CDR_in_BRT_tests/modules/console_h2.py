from selenium import webdriver
from selenium.webdriver.common.by import By
import pytest
import time

def open_h2_console(browser, calls_parameters):
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