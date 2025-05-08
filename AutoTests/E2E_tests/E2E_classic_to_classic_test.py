import time
from modules.work_browser import browser
from modules.db_connection import db_connection_brt
from modules.useful_functions import read_and_parse_file
from modules.work_db import get_amount_calls, get_balance_subscriber
from modules.console_h2 import open_h2_console

def test_add_cdrs(browser, db_connection_brt):
    filepath = 'test_e2e/classic_to_classic.txt'
    # раскладываем текстовый файл на массивы
    calls_parameters = read_and_parse_file(filepath)

    # получим количество звонков, которое было в БД Постгрес до добавления
    request = "SELECT * FROM cdrs;"
    start_amount_calls = get_amount_calls(db_connection_brt, request)

    # получим изначальный баланс каждого из абонентов
    start_balance_A1 = get_balance_subscriber(db_connection_brt, calls_parameters[0][1])
    start_balance_A2 = get_balance_subscriber(db_connection_brt, calls_parameters[0][2])

    # откроем браузер для доступа к консоли h2 и
    # введём нужные данные
    open_h2_console(browser, calls_parameters)
    # ждём, пока записи из H2 попадут в БРТ
    time.sleep(10)

    # проверяем, сколько записей стало посл добавления тестовых
    request = "SELECT * FROM cdrs;"
    end_amount_calls = get_amount_calls(db_connection_brt, request)

    # получим окончательный баланс каждого из абонентов
    end_balance_A1 = get_balance_subscriber(db_connection_brt, calls_parameters[0][1])
    end_balance_A2 = get_balance_subscriber(db_connection_brt, calls_parameters[0][2])
    
    # должны добавиться все записи из файла, кроме одной (9 штук)
    # десятая содержит звонок абонента с некорректной датой
    assert end_amount_calls - start_amount_calls == 10
    # баланс первого абонента уменьшился на нужную сумму
    assert start_balance_A1 - end_balance_A1 == 7.5
    # баланс второго абонента не изменился
    assert start_balance_A2 - end_balance_A2 == 0