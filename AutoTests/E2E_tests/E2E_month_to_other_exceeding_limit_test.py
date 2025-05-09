import time
from modules.work_browser import browser
from modules.db_connection import db_connection_brt, db_connection_hrs
from modules.work_db import get_amount_calls, get_balance_subscriber, get_used_minutes_subscriber
from modules.console_h2 import open_h2_console
from modules.generate_cdrs import generate_month_to_other_exceeding_limit_entries
from modules.durability_call import get_durability_call

def test_add_cdrs(browser, db_connection_brt, db_connection_hrs):
     # получим список из 10 звонков
    calls_parameters = []
    for call in (generate_month_to_other_exceeding_limit_entries(db_connection_brt, db_connection_hrs)):
        calls_parameters.append(call)

    # длительность тестового звонка
    durability_call = get_durability_call(calls_parameters)

    # получим количество звонков, которое было в БД Постгрес до добавления
    request = "SELECT * FROM cdrs;"
    start_amount_calls = get_amount_calls(db_connection_brt, request)

    # получим изначальный баланс каждого из абонентов
    start_balance_A1 = get_balance_subscriber(db_connection_brt, calls_parameters[9][1])

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

    end_minutes_A1 = get_used_minutes_subscriber(db_connection_brt, db_connection_hrs, calls_parameters[0][1])
    
    # должны добавиться все записи из файла
    assert end_amount_calls - start_amount_calls == 10
    # баланс абонента уменьшился на 125 у.е.
    assert start_balance_A1 - end_balance_A1 == 100 + (durability_call - 50) * 2.5
    # минуты у первого абонента не увеличились
    assert end_minutes_A1 == 50