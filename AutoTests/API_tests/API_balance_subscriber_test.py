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

# выборка валидной информации из БД
@pytest.fixture(scope="module")
def get_msisdn_balance_from_db(db_connection_brt, selected_tariff_id):
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

def test_subscriber_balance_replenishment(id_tariff = 2, sum_money = 10.00):
    try:
        # получим номер случайного абонента и его баланс
        test_msisdn, start_balance_subscriber = get_msisdn_balance_from_db(id_tariff)

        # производится авторизация абонента
        # Делаем запрос к API
        response = requests.post(API_URL, json={"msisdn": test_msisdn, "user": "subscriber"})
        # Проверяем статус код и JSON ответ
        assert response.status_code == 200
        data = response.json()
        # проверяем корректность полученного кода
        assert isinstance(data["code"], int)
        response = requests.post(API_URL, json={"msisdn": test_msisdn, "code": data["code"], "user": "subscriber"})
        # Проверяем, что авторизация успешна
        assert response.status_code == 200

        # производится пополнение баланса абонентом
        response = requests.put(API_URL, json={"phone": test_msisdn, "plus_balance": sum_money, "user": "subscriber"})
        # проверяем, что операция по пополнению баланса прошла успешно
        assert response.status_code == 200

        
        # получаем баланс абонента после его пополнения
        test_msisdn, end_balance_subscriber = get_msisdn_balance_from_db(id_tariff)

        # проверим, что значение баланса изменилось на нужну сумму
        assert end_balance_subscriber - start_balance_subscriber == sum_money
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")