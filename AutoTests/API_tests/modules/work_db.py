import pytest
import random
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

# выборка валидной информации из БД
def get_random_login_from_db(db_connection_crm):
    try:
        cur = db_connection_crm.cursor()
        # выберем данные для случайного менеджера
        cur.execute(f"SELECT login FROM app_users ORDER BY RANDOM() LIMIT 1;")
        for row in cur.fetchall():
            test_login = int(row[0])

    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        return test_login

# выборка валидной информации из БД
def get_info_from_db(db_connection_brt, db_connection_hrs, selected_tariff_id):
    try:
        cur = db_connection_brt.cursor()
        # выберем случайный номер телефона и id абонента
        cur.execute(f"SELECT id, msisdn FROM subscribers WHERE tariff_id = {selected_tariff_id} ORDER BY RANDOM() LIMIT 1;")
        for row in cur.fetchall():
            subscriber_id = int(row[0])
            test_msisdn = int(row[1])

        # получим информацию по абоненту
        cur.execute(f"SELECT * FROM subscribers WHERE msisdn = '{test_msisdn}';")
        info_subscriber = [row for row in cur.fetchall()]


        # подключаемся к БД HRS
        curs = db_connection_hrs.cursor()
        # находим название тарифа 
        curs.execute(f"SELECT name FROM tariffs WHERE id = {selected_tariff_id};")

        # добавим название тарифа к информации, которую нужно получить
        for row in curs.fetchall():
            info_subscriber.append(str(row[0]))

        # если тариф помесячный,
        # то нужно вывести количество использованных минут
        if selected_tariff_id == 2:
            # находим количество минут абонента
            curs.execute(f"""SELECT used_amount FROM subscriber_package_usage WHERE subscriber_id = {subscriber_id} 
                         AND is_deleted=False;""")
            # добавляем количество использованных минут в информацию об АБ
            for row in curs.fetchall():
                info_subscriber.append(int(row[0]))
        
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        return test_msisdn, info_subscriber

def get_random_msisdn_from_db(db_connection_brt, selected_tariff_id):
    try:
        cur = db_connection_brt.cursor()
        # выберем случайный номер телефона и id абонента
        cur.execute(f"SELECT msisdn FROM subscribers WHERE tariff_id = {selected_tariff_id} ORDER BY RANDOM() LIMIT 1;")
        for row in cur.fetchall():
            test_msisdn = int(row[0])

    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        return test_msisdn

# выборка валидной информации из БД
def get_msisdn_tariff_from_db(db_connection_brt, subscriber_msisdn):
    try:
        cur = db_connection_brt.cursor()
        # выберем случайный номер телефона и id абонента
        cur.execute(f"SELECT tariff_id FROM subscribers WHERE msisdn = {subscriber_msisdn} LIMIT 1;")
        for row in cur.fetchall():
            test_balance = int(row[0])
            test_msisdn = int(row[1])
        
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        return test_msisdn, test_balance

def get_random_msisdn_balance_from_db(db_connection_brt, selected_tariff_id):
    try:
        cur = db_connection_brt.cursor()
        # выберем случайный номер телефона и id абонента
        cur.execute(f"SELECT balance, msisdn FROM subscribers WHERE tariff_id = {selected_tariff_id} ORDER BY RANDOM() LIMIT 1;")
        for row in cur.fetchall():
            test_balance = int(row[0])
            test_msisdn = int(row[1])
        
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        return test_msisdn, test_balance

def get_new_msisdn_from_db(db_connection_brt):
    db_msisdn = []
    try:
        cur = db_connection_brt.cursor()
        # выберем все существующие в БД номера телефонов
        cur.execute(f"SELECT msisdn FROM subscribers;")
        for row in cur.fetchall():
            db_msisdn.append(row[0])
        
        # первоначально инициализируем номер телефона добавляемого абонента с уже существующим
        new_subscriber = db_msisdn[0]
        # генерируем значения номера
        # нужно, чтобы номер не был в списке абонентов Ромашки
        while new_subscriber in db_msisdn:
            new_subscriber = f'7{random.randint(900, 999)}{"".join(random.choices("0123456789", k=7))}'
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        return new_subscriber