import pytest
import psycopg2
# Раскладывает CDR-файл на записи, которые записывваются в список
def get_amount_calls(db_connection_brt, request):
    try:
        cur = db_connection_brt.cursor()
        cur.execute(request)
        cdrs_brt = [row for row in cur.fetchall()]
        start_amount_calls = len(cdrs_brt)
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    return start_amount_calls

# возвращает даты начала звонков для последних 10 звонков
def get_dates_cdr(db_connection_brt):
    try:
        cur = db_connection_brt.cursor()
        # получим список из даты и времен начала звонка
        # для последующей проверки хронологии
        cur.execute("SELECT start_date_time FROM (SELECT * FROM cdrs ORDER BY id DESC LIMIT 10) AS cdr ORDER BY id")
        dates_cdr = [row for row in cur.fetchall()]
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    return dates_cdr

# возвращает баланс абонента из БД
def get_balance_subscriber(db_connection_brt, test_msisdn):
    try:
        cur = db_connection_brt.cursor()
        cur.execute(f"SELECT balance FROM subscribers WHERE msisdn = '{test_msisdn}' LIMIT 1;")
        for row in cur.fetchall():
            balance = float(row[0])
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    return balance

# возвращает количество минут абонента из БД
def get_used_minutes_subscriber(db_connection_brt, db_connection_hrs, test_msisdn):
    try:
        cur = db_connection_brt.cursor()
        # нужно определить id абонентов для последующих запросов к бд HRS
        cur.execute(f"SELECT id FROM subscribers WHERE msisdn='{test_msisdn}' LIMIT 1;")  # Получаем список баз данных, исключая шаблоны
        for row in cur.fetchall():
            id_subscriber = int(row[0])

        # подключаемся к БД HRS
        curs = db_connection_hrs.cursor()
        curs.execute(f"""SELECT used_amount FROM subscriber_package_usage WHERE subscriber_id = {id_subscriber} 
                     AND is_deleted=False;""")  # Получаем список баз данных, исключая шаблоны
        for row in curs.fetchall():
            start_minutes = int(row[0])        
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    return start_minutes