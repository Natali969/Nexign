import pytest
import psycopg2
import requests
import random
from faker import Faker

fake = Faker('ru_RU')
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

def test_subscriber_balance_replenishment_by_manager(sum_money = 10.00):
    try:
        # получим новый номер для абонента
        test_msisdn = get_new_msisdn_from_db()
        
        # производится авторизация менеджера,
        # чтобы получить значение баланса абонента
        test_login = get_random_login_from_db()
        # Делаем запрос к API
        response = requests.post(API_URL, json={"login": test_login, "user": "manager"})
        # Проверяем, что авторизация успешна
        assert response.status_code == 200

        # проверяем, можем ли мы получить информацию об этом абоненте
        # Делаем запрос к API
        response = requests.get(API_URL, json={"msisdn": test_msisdn})
        # абонент должен быть не найден в системе
        assert response.status_code == 404

        # генерируем случайные данные для абонента
        new_tariff_id = random.randint(1, 2)

        # случайно выбираем пол абонента
        if gender is None:
            gender = random.choice(['male', 'female'])
    
        if gender == 'male':
            test_first_name = fake.first_name_male()
            test_last_name = fake.last_name_male()
            test_patronymic = fake.middle_name_male()
        elif gender == 'female':
            test_first_name = fake.first_name_female()
            test_last_name = fake.last_name_female()
            test_patronymic = fake.middle_name_female()

        # производится добавление нового абонента
        response = requests.post(API_URL, json={"msisdn": test_msisdn,
                                                "first_name": test_first_name,
                                                "second_name": test_patronymic,
                                                "surname": test_last_name,
                                                "tariff_id": new_tariff_id,
                                                "balance": 100.00})
        # проверяем, что операция по добавлению нового абонента прошла успешно
        assert response.status_code == 200

        # проверяем, можем ли мы получить информацию об этом абоненте
        # Делаем запрос к API
        response = requests.get(API_URL, json={"msisdn": test_msisdn})
        # абонент должен быть не найден в системе
        assert response.status_code == 200
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")