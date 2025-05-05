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


# проверка удаления абонента из системы
def test_api_get_subscriber_deleted():
    id_tariff = 2
    # получим тестовые данные
    # то, что мы отправляем, и что хотим получить
    test_msisdn = get_random_msisdn_from_db(id_tariff)

    # менеджер авторизуется,
    # чтобы выполнять запросы
    test_login = get_random_login_from_db()
    # Делаем запрос к API
    response = requests.post(API_URL, json={"login": test_login, "user": "manager"})
    # Проверяем, что авторизация успешна
    assert response.status_code == 200
    
    # удаляем этого абонента
    response = requests.delete(API_URL, json={"msisdn": test_msisdn})  # Несуществующий номер
    # проверяем, что удаление прошло успешно
    assert response.status_code == 200
    # проверяем, можем ли мы получить информацию об этом абоненте
    # Делаем запрос к API
    response = requests.get(API_URL, json={"msisdn": test_msisdn, "user": "manager"})
    # абонент должен быть не найден в системе
    assert response.status_code == 403