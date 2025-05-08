import pytest
import psycopg2
import requests
from modules.db_connection import db_connection_crm
from modules.work_db import get_random_login_from_db

# тестовый URL
# когда будет готова API, заменить на настоящий
API_URL = "http://localhost:8060/api/"

def test_subscriber_autorization(db_connection_crm):
    try:
        # получим тестовые данные
        # то, что мы отправляем, и что хотим получить
        test_login = get_random_login_from_db(db_connection_crm)
        # Делаем запрос к API
        response = requests.post(API_URL, json={"login": test_login, "user": "manager"})
        
        # Проверяем, что авторизация успешна
        assert response.status_code == 200
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")