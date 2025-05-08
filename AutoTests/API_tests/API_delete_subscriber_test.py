import requests
from modules.db_connection import db_connection_brt, db_connection_crm
from modules.work_db import get_random_msisdn_from_db, get_random_login_from_db

# тестовый URL
# когда будет готова API, заменить на настоящий
API_URL = "http://localhost:8060/api/"

# проверка удаления абонента из системы
def test_api_get_subscriber_deleted(db_connection_brt, db_connection_crm):
    id_tariff = 2
    # получим тестовые данные
    # то, что мы отправляем, и что хотим получить
    test_msisdn = get_random_msisdn_from_db(db_connection_brt, id_tariff)

    # менеджер авторизуется,
    # чтобы выполнять запросы
    test_login = get_random_login_from_db(db_connection_crm)
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