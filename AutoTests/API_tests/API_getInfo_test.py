import pytest
import psycopg2
import requests
from modules.db_connection import db_connection_brt, db_connection_hrs, db_connection_crm
from modules.work_db import get_info_from_db, get_random_login_from_db

# тестовый URL
# когда будет готова API, заменить на настоящий
API_URL = "http://localhost:8060/api/"

def test_get_info(db_connection_brt, db_connection_hrs, db_connection_crm):   
    try:
        id_tariff = 2
        # получим тестовые данные
        # то, что мы отправляем, и что хотим получить
        test_msisdn, info_subscriber = get_info_from_db(db_connection_brt, db_connection_hrs, id_tariff)

        # менеджер авторизуется,
        # чтобы выполнять запросы
        test_login = get_random_login_from_db(db_connection_crm)
        # Делаем запрос к API
        response = requests.post(API_URL, json={"login": test_login, "user": "manager"})
        
        # Проверяем, что авторизация успешна
        assert response.status_code == 200

        # Делаем запрос к API
        response = requests.get(API_URL, json={"msisdn": test_msisdn, "user": "manager"})

        # Проверяем статус код и JSON ответ
        assert response.status_code == 200
        data = response.json()
        # проверяем корректность полученных данных
        assert data["id_subscriber"] == info_subscriber[0]
        assert data["first_name"] == info_subscriber[1]
        assert data["second_name"] == info_subscriber[2]
        assert data["surname"] == info_subscriber[3]
        assert data["tariff_id"] == info_subscriber[4]
        assert data["balance"] == info_subscriber[5]
        assert data["registered_at"] == info_subscriber[6]
        assert data["tariff_name"] == info_subscriber[7]
        if data["tariff_id"] == 2:
            assert data["used_minutes"] == info_subscriber[8]
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")