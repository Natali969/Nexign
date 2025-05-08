import pytest
import psycopg2
import requests
from modules.db_connection import db_connection_brt
from modules.work_db import get_random_msisdn_from_db

# тестовый URL
# когда будет готова API, заменить на настоящий
API_URL = "http://localhost:8060/api/"

def test_subscriber_autorization(db_connection_brt):
    try:
        id_tariff = 2
        # получим тестовые данные
        # то, что мы отправляем, и что хотим получить
        test_msisdn = get_random_msisdn_from_db(db_connection_brt, id_tariff)
        # Делаем запрос к API
        response = requests.post(API_URL, json={"phone": test_msisdn, "user": "subscriber"})
        # Проверяем статус код и JSON ответ
        assert response.status_code == 200

        id_tariff = 1
        # получим тестовые данные
        # то, что мы отправляем, и что хотим получить
        test_msisdn = get_random_msisdn_from_db(db_connection_brt, id_tariff)
        # Делаем запрос к API
        response = requests.post(API_URL, json={"phone": test_msisdn, "user": "subscriber"})
        # Проверяем статус код и JSON ответ
        assert response.status_code == 200
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")