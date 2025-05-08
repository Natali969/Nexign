import pytest
import psycopg2
import requests
from modules.db_connection import db_connection_brt, db_connection_crm
from modules.work_db import get_random_msisdn_from_db, get_random_login_from_db, get_msisdn_tariff_from_db

# тестовый URL
# когда будет готова API, заменить на настоящий
API_URL = "http://localhost:8060/api/"



def test_subscriber_balance_replenishment_by_manager(db_connection_brt, db_connection_crm):
    try:
        id_tariff = 2
        sum_money = 10.00
        # получим номер случайного абонента и его баланс
        test_msisdn = get_random_msisdn_from_db(db_connection_brt, id_tariff)
        
        # получаем тариф абонента до его смены
        start_id_tariff = get_msisdn_tariff_from_db(db_connection_brt, test_msisdn)
        # производится авторизация менеджера,
        # чтобы получить значение баланса абонента
        test_login = get_random_login_from_db(db_connection_crm)
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
        end_id_tariff = get_msisdn_tariff_from_db(db_connection_brt, test_msisdn)

        # проверим, что значение баланса изменилось на нужную сумму
        assert start_id_tariff != end_id_tariff
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")