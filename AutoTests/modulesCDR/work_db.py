import psycopg2
from psycopg2 import Error
# Раскладывает CDR-файл на записи, которые записывваются в список
def get_subscribers_list(request):
    # список номеров абонентов "Ромашки" из БД
    subscribers_list = []
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(
            host="localhost",
            database="brt-db",  # Подключаемся к базе данных "postgres",  к которой всегда есть доступ
            user="postgres",
            password="postgres",
            port="5432"
        )

        cursor = connection.cursor()
        postgreSQL_select_Query = "SELECT msisdn FROM subscribers LiMIT 10"
        cursor.execute(postgreSQL_select_Query)
        # Выбор строк из таблицы с абонентами с помощью cursor.fetchall
        subscribers_records = cursor.fetchall()

        # добавляем номер каждого из абонентов в список
        for row in subscribers_records:
            subscribers_list.append(str(row[0]))
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")
        return subscribers_list

# Раскладывает CDR-файл на записи, которые записывваются в список
def get_subscribers_classsic_list():
    # список номеров абонентов "Ромашки" из БД
    subscribers_list = []
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(
            host="localhost",
            database="brt-db",  # Подключаемся к базе данных "postgres",  к которой всегда есть доступ
            user="postgres",
            password="postgres",
            port="5432"
        )

        cursor = connection.cursor()
        postgreSQL_select_Query = "SELECT msisdn FROM subscribers WHERE tariff_id = 1 LiMIT 10"
        cursor.execute(postgreSQL_select_Query)
        # Выбор строк из таблицы с абонентами с помощью cursor.fetchall
        subscribers_records = cursor.fetchall()

        # добавляем номер каждого из абонентов в список
        for row in subscribers_records:
            subscribers_list.append(str(row[0]))
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")
        return subscribers_list
