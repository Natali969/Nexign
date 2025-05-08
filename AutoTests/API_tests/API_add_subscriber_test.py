import pytest
import psycopg2
import requests
from modules.db_connection import db_connection_brt, db_connection_crm
from modules.work_db import get_new_msisdn_from_db, get_random_login_from_db
from modules.generate_new_subscriver_info import generate_random_info

# тестовый URL
# когда будет готова API, заменить на настоящий
API_URL = "http://localhost:8060/api/"


def test_subscriber_balance_replenishment_by_manager(db_connection_brt, db_connection_crm):
    try:
        # получим новый номер для абонента
        test_msisdn = get_new_msisdn_from_db(db_connection_brt)
        
        # производится авторизация менеджера,
        # чтобы получить значение баланса абонента
        test_login = get_random_login_from_db(db_connection_crm)
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
        new_tariff_id, test_first_name, test_last_name, test_patronymic = generate_random_info()


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