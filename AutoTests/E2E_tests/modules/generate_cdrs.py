import random
import datetime
from faker import Faker
from modules.db_connection import db_connection_brt, db_connection_hrs
from modules.work_db import get_subscribers_list
from modules.calls_intersect import is_calls_intersect
import pytest
import psycopg2
from psycopg2 import Error

fake = Faker('ru_RU')
outgoing_call = '01'
ingoing_call = '02'

# список номеров абонентов "Ромашки" из БД
request = "SELECT msisdn FROM subscribers LiMIT 10"
subscribers_list = get_subscribers_list(request)

# список номеров абонентов "Ромашки" из БД с тарифом Классика
request = "SELECT msisdn FROM subscribers WHERE tariff_id = 1 LiMIT 10"
subscribers_classic_list = get_subscribers_list(request)
print(subscribers_classic_list)
# список номеров абонентов "Ромашки" из БД с тарифом Помесячный
request = "SELECT msisdn FROM subscribers WHERE tariff_id = 2 LiMIT 10"
subscribers_month_list = get_subscribers_list(request)

# генерирует запись, исходящую от нашего абонента абоненту другого оператора
def generate_cdr_from_our_to_other(subscribers_list):
    # тип вызова: исходящий или входящий
    call_type = random.choice([outgoing_call, ingoing_call])

    # номер абонента должен выбираться из БД запросом SELECT,
    # а дальше - рандомный выбор в коде

    # обслуживаемый абонент выбирается из БД
    our_subscriber = random.choice(subscribers_list)

    other_subscriber = f'7921{"".join(random.choices("0123456789", k=7))}'

    # генерируется дата и время начала звонка
    # date_part = fake.date_time_between(start_date=start_date, end_date=end_date).date()
    date_part = fake.date_between(start_date='-1y', end_date='-1d')
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    
    start_datetime = datetime.datetime(date_part.year, date_part.month, date_part.day, hour, minute, second)

    # продолжительность звонка от 1 секунды до получаса
    duration = random.randint(1, 1800)

    # дата и время окончания звонка
    end_datetime = start_datetime + datetime.timedelta(seconds=duration)

    # генерируем запись
    entries = []
    entries.append([call_type, our_subscriber, other_subscriber,
                            start_datetime.isoformat(), end_datetime.isoformat()])
    return entries

# генерирует запись, исходящую от нашего абонента абоненту нашего оператора
def generate_cdr_from_our_to_our(subscribers_list, subscribers_other_list):
    # тип вызова: исходящий или входящий
    call_type = outgoing_call

    # номер абонента должен выбираться из БД запросом SELECT,
    # а дальше - рандомный выбор в коде

    # обслуживаемый абонент выбирается из БД
    our_subscriber = random.choice(subscribers_list)
    # второй также выбирается из БД
    other_subscriber = random.choice(subscribers_other_list)

    # генерируется дата и время начала звонка
    date_part = fake.date_between(start_date='-1y', end_date='-1d')
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    
    start_datetime = datetime.datetime(date_part.year, date_part.month, date_part.day, hour, minute, second)

    # продолжительность звонка от 1 секунды до получаса
    duration = random.randint(1, 1800)

    # дата и время окончания звонка
    end_datetime = start_datetime + datetime.timedelta(seconds=duration)

    # генерируем запись
    entries = []
    entries.append([call_type, our_subscriber, other_subscriber,
                    start_datetime.isoformat(), end_datetime.isoformat()])
            
    call_type_anti = ingoing_call if call_type == outgoing_call else outgoing_call

    entries.append([call_type_anti, other_subscriber, our_subscriber,
                    start_datetime.isoformat(), end_datetime.isoformat()])
    return entries


def generate_classic_to_classic_entries():
    # отсортированные записи о звонках для всех n файлов
    all_entries = []
    while len(all_entries) < 1:
        # добавим зеркальный звонок с классики на помесячный (2 записи)
        gen_cdrs = generate_cdr_from_our_to_our(subscribers_classic_list, subscribers_classic_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                all_entries.append(cdr)
    while len(all_entries) < 10:
        gen_cdrs = generate_cdr_from_our_to_other(subscribers_classic_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                cdr[0] = ingoing_call
                all_entries.append(cdr)

    # отсортируем звонки по дате  ивремени их начала
    # sorted_entries = sorted(all_entries, key=lambda x: datetime.datetime.strptime(x[3], '%Y-%m-%dT%H:%M:%S'))
    return all_entries

def generate_classic_to_other_entries():
    # отсортированные записи о звонках для всех n файлов
    all_entries = []
    while len(all_entries) < 1:
        # добавим зеркальный звонок с классики на помесячный (2 записи)
        gen_cdrs = generate_cdr_from_our_to_other(subscribers_classic_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                cdr[0] = outgoing_call
                all_entries.append(cdr)
    while len(all_entries) < 10:
        gen_cdrs = generate_cdr_from_our_to_other(subscribers_classic_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                cdr[0] = ingoing_call
                all_entries.append(cdr)

    # отсортируем звонки по дате  ивремени их начала
    # sorted_entries = sorted(all_entries, key=lambda x: datetime.datetime.strptime(x[3], '%Y-%m-%dT%H:%M:%S'))
    return all_entries
      
# получить списки абонентов согласно их количеству минут
def get_subscribers_case_minutes(db_connection_brt, db_connection_hrs):
    try:
        cur = db_connection_brt.cursor()
        # нужно определить id абонентов для последующих запросов к бд HRS
        cur.execute(f"SELECT Id FROM subscribers WHERE tariff_id=2;")  # Получаем список баз данных, исключая шаблоны
        id_subscribers = '('
        rows = cur.fetchall()
        for i, row in enumerate(rows):
            id_subscribers += str(row[0])
            if i != len(rows) - 1:
                id_subscribers += ', '
        id_subscribers += ')'
        # подключаемся к БД HRS
        curs = db_connection_hrs.cursor()
        curs.execute(f"""SELECT subscriber_id FROM subscriber_package_usage WHERE subscriber_id IN {id_subscribers}
                     AND is_deleted=False AND used_amount < 20;""")
        
        id_subscribers_more_minutes = '('
        rows = curs.fetchall()
        for i, row in enumerate(rows):
            id_subscribers_more_minutes += str(row[0])
            if i != len(rows) - 1:
                id_subscribers_more_minutes += ', '
        id_subscribers_more_minutes += ')'

        curs.execute(f"""SELECT subscriber_id FROM subscriber_package_usage WHERE subscriber_id IN {id_subscribers} 
                     AND is_deleted=False AND used_amount = 50;""")
        # сделать строку вида (...)
        id_subscribers_no_minutes = '('
        rows = curs.fetchall()
        for i, row in enumerate(rows):
            id_subscribers_no_minutes += str(row[0])
            if i != len(rows) - 1:
                id_subscribers_no_minutes += ', '
        id_subscribers_no_minutes += ')'
        cur.execute(f"SELECT msisdn FROM subscribers WHERE Id IN {id_subscribers_more_minutes};")
        list_subscribers_more_minutes = []
        for row in cur.fetchall():
            list_subscribers_more_minutes.append(int(row[0]))
        cur.execute(f"SELECT msisdn FROM subscribers WHERE Id IN {id_subscribers_no_minutes};")
        list_subscribers_no_minutes = []
        for row in cur.fetchall():
            list_subscribers_no_minutes.append(int(row[0]))

    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    return list_subscribers_more_minutes, list_subscribers_no_minutes
# С помесячного на помесячный
# У того, кому звоним (3 колонка) - минут дофига  - длительность разговора должна прибавиться
# У того, кто звонит (2 колонка) - минуты закончились - минуты не прибавляются
# Длительность разговора - с 13:55:00 до 13:59:00 - 4 минуты 
# Плата списывается по тарифу Классики 4 * 1.5 = 6 у.е.
# Две зеркальные записи
# Остальные 8 записей в CDR - входящие звонки абонентам с Классикой
def generate_month_to_month_entries(db_connection_brt, db_connection_hrs):
    # отсортированные записи о звонках для всех n файлов
    all_entries = []
    request = "SELECT msisdn FROM subscribers WHERE tariff_id = 2 LiMIT 10"
    subscribers_month_list = get_subscribers_list(request)
    subscribers_month_more_minutes, subscribers_month_no_minutes = get_subscribers_case_minutes(db_connection_brt, db_connection_hrs)

    while len(all_entries) < 1:
        # добавим зеркальный звонок с помесячного на помесячный (2 записи)
        gen_cdrs = generate_cdr_from_our_to_our(subscribers_month_no_minutes, subscribers_month_more_minutes)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                all_entries.append(cdr)
    while len(all_entries) < 10:
        gen_cdrs = generate_cdr_from_our_to_other(subscribers_classic_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                cdr[0] = ingoing_call
                all_entries.append(cdr)

    # отсортируем звонки по дате  ивремени их начала
    # sorted_entries = sorted(all_entries, key=lambda x: datetime.datetime.strptime(x[3], '%Y-%m-%dT%H:%M:%S'))
    return all_entries

def generate_month_to_other_exceeding_limit_entries(db_connection_brt, db_connection_hrs):
    # отсортированные записи о звонках для всех n файлов
    all_entries = []
    request = "SELECT msisdn FROM subscribers WHERE tariff_id = 2 LiMIT 10"
    subscribers_month_list = get_subscribers_list(request)
    subscribers_month_more_minutes, subscribers_month_no_minutes = get_subscribers_case_minutes(db_connection_brt, db_connection_hrs)

    while len(all_entries) < 1:
        # добавим  звонок с помесячного на другого оператора
        gen_cdrs = generate_cdr_from_our_to_other(subscribers_month_no_minutes)
        gen_cdrs[0][3] = "2025-05-18T08:15:39"
        gen_cdrs[0][4] = "2025-05-18T09:15:34"
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                cdr[0] = outgoing_call
                all_entries.append(cdr)
    while len(all_entries) < 10:
        gen_cdrs = generate_cdr_from_our_to_other(subscribers_classic_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                cdr[0] = ingoing_call
                all_entries.append(cdr)

    # отсортируем звонки по дате  ивремени их начала
    # sorted_entries = sorted(all_entries, key=lambda x: datetime.datetime.strptime(x[3], '%Y-%m-%dT%H:%M:%S'))
    return all_entries