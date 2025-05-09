import time
from modules.work_browser import browser
from modules.db_connection import db_connection_brt, db_connection_hrs
from modules.work_db import get_amount_calls, get_balance_subscriber, get_used_minutes_subscriber
from modules.console_h2 import open_h2_console
from modules.generate_cdrs import generate_month_to_month_entries
from modules.durability_call import get_durability_call

def test_add_cdrs(browser, db_connection_brt, db_connection_hrs):
     # получим список из 10 звонков
    calls_parameters = []
    for call in (generate_month_to_month_entries(db_connection_brt, db_connection_hrs)):
        calls_parameters.append(call)
    print(calls_parameters)
    # длительность тестового звонка
    durability_call = get_durability_call(calls_parameters)

    # получим количество звонков, которое было в БД Постгрес до добавления
    request = "SELECT * FROM cdrs;"
    start_amount_calls = get_amount_calls(db_connection_brt, request)

    # получим изначальный баланс каждого из абонентов
    start_balance_A1 = get_balance_subscriber(db_connection_brt, calls_parameters[0][1])
    start_balance_A2 = get_balance_subscriber(db_connection_brt, calls_parameters[0][2])

    start_minutes_A1 = get_used_minutes_subscriber(db_connection_brt, db_connection_hrs, calls_parameters[0][1])
    start_minutes_A2 = get_used_minutes_subscriber(db_connection_brt, db_connection_hrs, calls_parameters[0][2])

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

    end_minutes_A1 = get_used_minutes_subscriber(db_connection_brt, db_connection_hrs, calls_parameters[0][1])
    end_minutes_A2 = get_used_minutes_subscriber(db_connection_brt, db_connection_hrs, calls_parameters[0][2])
    
    # должны добавиться все записи из файла
    assert end_amount_calls - start_amount_calls == 10
    # баланс первого абонента уменьшился на длительность звонка * 1.5 у.е = 4 * 1.5 = 6
    assert start_balance_A1 - end_balance_A1 == durability_call * 1.5
    # баланс второго абонента не изменился
    assert start_balance_A2 - end_balance_A2 == 0
    # минуты у первого абонента не увеличились
    assert end_minutes_A1 - start_minutes_A1 == 0
    # минуты у второго абонента увеличились на 4
    assert end_minutes_A2 - start_minutes_A2 == durability_call