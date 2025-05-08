import time
from modules.work_browser import browser
from modules.db_connection import db_connection_brt
from modules.useful_functions import read_and_parse_file
from modules.work_db import get_amount_calls
from modules.console_h2 import open_h2_console

def test_add_cdrs(browser, db_connection_brt):
    filepath = 'test_cdr/positive_cdr1.txt'
    # раскладываем текстовый файл на массивы
    calls_parameters = read_and_parse_file(filepath)

    # получим количество звонков, которое было в БД Постгрес до добавления
    request = "SELECT * FROM cdrs;"
    start_amount_calls = get_amount_calls(db_connection_brt, request)

    # откроем браузер для доступа к консоли h2 и
    # введём нужные данные
    open_h2_console(browser, calls_parameters)

    # ждём, пока записи из H2 попадут в БРТ
    time.sleep(10)

    request = "SELECT * FROM cdrs;"
    end_amount_calls = get_amount_calls(db_connection_brt, request)

    # должны добавиться все записи из файла
    assert end_amount_calls - start_amount_calls == 10