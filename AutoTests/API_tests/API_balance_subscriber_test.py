import pytest
import psycopg2
import requests
from modules.db_connection import db_connection_brt
from modules.work_db import get_random_msisdn_balance_from_db, get_balance_subscriber

# тестовый URL
# когда будет готова API, заменить на настоящий
API_URL = "http://localhost:8060/api/"


def test_subscriber_balance_replenishment(db_connection_brt):
    try:
        id_tariff = 2
        sum_money = 10.00
        # получим номер случайного абонента и его баланс
        test_msisdn, start_balance_subscriber = get_random_msisdn_balance_from_db(db_connection_brt, id_tariff)

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
        test_msisdn, end_balance_subscriber = get_balance_subscriber(db_connection_brt, test_msisdn)

        # проверим, что значение баланса изменилось на нужну сумму
        assert end_balance_subscriber - start_balance_subscriber == sum_money
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")