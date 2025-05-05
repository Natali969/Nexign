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

# подключение к БД postgres HRS
@pytest.fixture(scope="module")
def db_connection_hrs():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="hrs-db",
            user="postgres",
            password="postgres",
            port="5433")
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
def get_info_from_db(db_connection_brt, db_connection_hrs, selected_tariff_id):
    # selected_tariff_id = 2
    try:
        cur = db_connection_brt.cursor()
        # выберем случайный номер телефона и id абонента
        cur.execute(f"SELECT id, msisdn FROM subscribers WHERE tariff_id = {selected_tariff_id} ORDER BY RANDOM() LIMIT 1;")
        for row in cur.fetchall():
            subscriber_id = int(row[0])
            test_msisdn = int(row[1])

        # получим информацию по абоненту
        cur.execute(f"SELECT * FROM subscribers WHERE msisdn = '{test_msisdn}';")
        info_subscriber = [row for row in cur.fetchall()]


        # подключаемся к БД HRS
        curs = db_connection_hrs.cursor()
        # находим название тарифа 
        curs.execute(f"SELECT name FROM tariffs WHERE id = {selected_tariff_id};")

        # добавим название тарифа к информации, которую нужно получить
        for row in curs.fetchall():
            info_subscriber.append(str(row[0]))

        # если тариф помесячный,
        # то нужно вывести количество использованных минут
        if selected_tariff_id == 2:
            # находим количество минут абонента
            curs.execute(f"""SELECT used_amount FROM subscriber_package_usage WHERE subscriber_id = {subscriber_id} 
                         AND is_deleted=False;""")
            # добавляем количество использованных минут в информацию об АБ
            for row in curs.fetchall():
                info_subscriber.append(int(row[0]))
        
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    finally:
        return test_msisdn, info_subscriber

def test_get_info():   
    try:
        id_tariff = 2
        # получим тестовые данные
        # то, что мы отправляем, и что хотим получить
        test_msisdn, info_subscriber = get_info_from_db(id_tariff)

        # менеджер авторизуется,
        # чтобы выполнять запросы
        test_login = get_random_login_from_db()
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
        if id_tariff == 2:
            assert data["used_minutes"] == info_subscriber[8]
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при подключении к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")