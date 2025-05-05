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

def test_subscriber_autorization():
    try:
        id_tariff = 2
        # получим тестовые данные
        # то, что мы отправляем, и что хотим получить
        test_msisdn = get_random_msisdn_from_db(id_tariff)
        # Делаем запрос к API
        response = requests.post(API_URL, json={"phone": test_msisdn, "user": "subscriber"})
        # Проверяем статус код и JSON ответ
        assert response.status_code == 200
        data = response.json()
        # проверяем корректность полученного кода
        assert isinstance(data["code"], int)
        
        response = requests.post(API_URL, json={"phone": test_msisdn, "code": data["code"], "user": "subscriber"})
        # Проверяем, что авторизация успешна
        assert response.status_code == 200
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")