import pytest
import psycopg2
import requests

# тестовый URL
# когда будет готова API, заменить на настоящий
API_URL = "http://localhost:8060/api/"

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

# подключение к БД postgres CRM
@pytest.fixture(scope="module")
def db_connection_crm():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="crm-db",
            user="postgres",
            password="postgres",
            port="5435")
        yield conn
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        if conn:
            conn.close()

# выборка валидной информации из БД
@pytest.fixture(scope="module")
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
@pytest.fixture(scope="module")
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

# выборка валидной информации из БД
@pytest.fixture(scope="module")
def get_random_msisdn_from_db(db_connection_brt, selected_tariff_id):
    try:
        cur = db_connection_brt.cursor()
        # выберем случайный номер телефона
        cur.execute(f"SELECT msisdn FROM subscribers WHERE tariff_id = {selected_tariff_id} ORDER BY RANDOM() LIMIT 1;")
        for row in cur.fetchall():
            test_msisdn = int(row[0])
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        return test_msisdn

def test_subscriber_balance_replenishment_by_manager(id_tariff = 2, sum_money = 10.00):
    try:
        # получим номер случайного абонента и его баланс
        test_msisdn = get_random_msisdn_from_db(id_tariff)
        
        # получаем тариф абонента до его смены
        start_id_tariff = get_msisdn_tariff_from_db(test_msisdn)
        # производится авторизация менеджера,
        # чтобы получить значение баланса абонента
        test_login = get_random_login_from_db()
        # Делаем запрос к API
        response = requests.post(API_URL, json={"login": test_login, "user": "manager"})
        # Проверяем, что авторизация успешна
        assert response.status_code == 200

        new_id_tariff = 1 if id_tariff == 2 else 2
        # производится смена тарифа менеджером
        response = requests.put(API_URL, json={"phone": test_msisdn, "id_tariff": new_id_tariff, "user": "manager"})
        # проверяем, что операция по смене тарифа прошла успешно
        assert response.status_code == 200

        # получаем тариф абонента после его смены
        end_id_tariff = get_msisdn_tariff_from_db(test_msisdn)

        # проверим, что значение баланса изменилось на нужную сумму
        assert start_id_tariff != end_id_tariff
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")