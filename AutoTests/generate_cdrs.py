import random
import datetime
from faker import Faker
from psycopg2 import Error
from modulesCDR.db_connection import db_connection_brt
from modulesCDR.work_db import get_subscribers_list
from modulesCDR.calls_intersect import is_calls_intersect


fake = Faker('ru_RU')
outgoing_call = '01'
ingoing_call = '02'
subscribers_list = get_subscribers_list("SELECT msisdn FROM subscribers LiMIT 10")

# список номеров абонентов "Ромашки" из БД
request = "SELECT msisdn FROM subscribers LiMIT 10"
subscribers_list = get_subscribers_list(request)

# список номеров абонентов "Ромашки" из БД с тарифом Классика
request = "SELECT msisdn FROM subscribers WHERE tariff_id = 1 LiMIT 10"
subscribers_classic_list = get_subscribers_list(request)

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

# генерирует запись, исходящую от нашего абонента абоненту другого оператора
def generate_cdr_from_our_to_our(subscribers_list, subscribers_other_list):
    # тип вызова: исходящий или входящий
    call_type = random.choice([outgoing_call, ingoing_call])

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

# генерирует запись, исходящую от нашего абонента абоненту другого оператора
def generate_cdr_from_our_to_our_midnight(subscribers_list, subscribers_other_list):
    # тип вызова: исходящий или входящий
    call_type = random.choice([outgoing_call, ingoing_call])

    # номер абонента должен выбираться из БД запросом SELECT,
    # а дальше - рандомный выбор в коде

    # обслуживаемый абонент выбирается из БД
    our_subscriber = random.choice(subscribers_list)
    # второй также выбирается из БД
    other_subscriber = random.choice(subscribers_other_list)

    # генерируется дата и время начала звонка
    date_part = fake.date_between(start_date='-1y', end_date='-1d')
    hour = 23
    minute = random.randint(50, 59)
    second = random.randint(0, 59)
    
    start_datetime = datetime.datetime(date_part.year, date_part.month, date_part.day, hour, minute, second)

    # продолжительность звонка от 10 минут и 1 секунды до получаса
    duration = random.randint(601, 1800)

    # дата и время окончания звонка
    end_datetime = start_datetime + datetime.timedelta(seconds=duration)

    # генерируем записи
    entries = []
    call_type_anti = ingoing_call if call_type == outgoing_call else outgoing_call
    midnight = datetime.datetime(start_datetime.year, start_datetime.month, start_datetime.day, 23, 59, 59)
    after_midnight = datetime.datetime(end_datetime.year, end_datetime.month, end_datetime.day, 0, 0, 0)
    entries.append([call_type, our_subscriber, other_subscriber,
                    start_datetime.isoformat(), midnight.isoformat()])
    entries.append([call_type_anti,other_subscriber, our_subscriber,
                    start_datetime.isoformat(), midnight.isoformat()])
    
    entries.append([call_type, our_subscriber, other_subscriber,
                    after_midnight.isoformat(), end_datetime.isoformat()])
    entries.append([call_type_anti,other_subscriber, our_subscriber,
                    after_midnight.isoformat(), end_datetime.isoformat()])
    return entries

def generate_all_entries(num_cdr, num_records = 10):
    # отсортированные записи о звонках для всех n файлов
    all_entries = []
    while len(all_entries) < num_cdr * num_records:
        entries = []
        # добавим одиночный звонок, совершаемый между Помесячным и другим оператором
        gen_cdrs = generate_cdr_from_our_to_other(subscribers_month_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        # если сгенерированный звонок не перескается с уже существующими, то добавляем его
        if calls_intersect == False:
            for cdr in gen_cdrs:
                all_entries.append(cdr)
        # добавим одиночный звонок, совершаемый между Классикой и другим оператором
        gen_cdrs = generate_cdr_from_our_to_other(subscribers_classic_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                all_entries.append(cdr)
        # добавим зеркальный звонок с классики на помесячный (2 записи)
        gen_cdrs = generate_cdr_from_our_to_our(subscribers_classic_list, subscribers_month_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                all_entries.append(cdr)
        # добавим зеркальный звонок с помесячного на классику (2 записи)
        gen_cdrs = generate_cdr_from_our_to_our(subscribers_month_list, subscribers_classic_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=False)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                all_entries.append(cdr)
        # добавим зеркальный звонок, пересекающий границу полуночи (4 записи)
        gen_cdrs = generate_cdr_from_our_to_our_midnight(subscribers_list, subscribers_list)
        calls_intersect = is_calls_intersect(all_entries, gen_cdrs, is_call_midnight=True)
        if calls_intersect == False:
            for cdr in gen_cdrs:
                all_entries.append(cdr)
    # отсортируем звонки по дате  ивремени их начала
    sorted_entries = sorted(all_entries, key=lambda x: datetime.datetime.strptime(x[3], '%Y-%m-%dT%H:%M:%S'))
    return sorted_entries

# функция для записи CDR-файла
def generate_cdr_file(num_cdr):
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(num_cdr)
    # сгенерировать нужное количество файлов
    for num_file in range(num_cdr):
        # пронумеровать все файлы
        filename = f"test_cdr/positive_cdr{num_file + 1}.txt"
        # вставляем по 10 записей в каждый файл
        with open(filename, 'w') as f:
            for entry in all_entries[num_file * 10 : (num_file + 1) * 10]:
                record = f"{entry[0]},{entry[1]},{entry[2]},{entry[3]},{entry[4]}"
                f.write(record + '\n')

# функция для создания негативных тестов
# с некорректными датой и временем звонка
def generate_negative_incorrect_datetime():
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(1)
    # изменяется дата и время начала звонка
    # она должна быть больше даты и времени окончания звонка
    duration = random.randint(1, 1800)
    all_entries[0][3] = (datetime.datetime.strptime(all_entries[0][4],
                                                   '%Y-%m-%dT%H:%M:%S') + datetime.timedelta(seconds=duration)).isoformat()
    # данный файл сохраняется
    filename = f"test_cdr/negative_cdr_incorrect_date.txt"
    # вставляем по 10 записей в каждый файл
    with open(filename, 'w') as f:
        for entry in all_entries:
            record = f"{entry[0]},{entry[1]},{entry[2]},{entry[3]},{entry[4]}"
            f.write(record + '\n')

# функция для создания негативных тестов
# ни один из абонентов не принадлежит Ромашке
def generate_negative_not_romashka():
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(1)

    # переменные для генерации неправильных абонентов
    # временно инициализируем их номерами абонентов Ромашки
    not_our_subscriber, not_other_subscriber = subscribers_list[0], subscribers_list[0]

    # генерируем значения номеров
    # нужно, чтобы оба номера не были в списке абонентов Ромашки
    while not_our_subscriber in subscribers_list:
        not_our_subscriber = f'7{random.randint(900, 999)}{"".join(random.choices("0123456789", k=7))}'

    while not_other_subscriber in subscribers_list:
        not_other_subscriber = f'7{random.randint(900, 999)}{"".join(random.choices("0123456789", k=7))}'
    
    # заменяем правильные номера неправильными
    all_entries[0][1] = not_our_subscriber
    all_entries[0][2] = not_other_subscriber
    # данный файл сохраняется
    filename = f"test_cdr/negative_cdr_not_romashka.txt"
    # вставляем по 10 записей в файл
    # первая - невалидная
    with open(filename, 'w') as f:
        for entry in all_entries:
            record = f"{entry[0]},{entry[1]},{entry[2]},{entry[3]},{entry[4]}"
            f.write(record + '\n')

# функция для создания негативных тестов
# абонент разговаривает сам с собой
def generate_negative_call_myself():
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(1)

    all_entries[0][2] = all_entries[0][1]
    # данный файл сохраняется
    filename = f"test_cdr/negative_cdr_call_myself.txt"
    # вставляем по 10 записей в файл
    # первая - невалидная
    with open(filename, 'w') as f:
        for entry in all_entries:
            record = f"{entry[0]},{entry[1]},{entry[2]},{entry[3]},{entry[4]}"
            f.write(record + '\n')

# функция для создания негативных тестов
# записи отсортированы по убыванию даты и времени начала звонка
def generate_negative_incorrect_chronology():
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(1)

    all_entries = reversed(all_entries)
    # данный файл сохраняется
    filename = f"test_cdr/negative_cdr_incorrect_chronology.txt"
    # вставляем по 10 записей в файл
    # первая - невалидная
    with open(filename, 'w') as f:
        for entry in all_entries:
            record = f"{entry[0]},{entry[1]},{entry[2]},{entry[3]},{entry[4]}"
            f.write(record + '\n')

# функция для создания негативных тестов
# записи отсортированы по убыванию даты и времени начала звонка
def generate_negative_through_midnight():
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(1)

    start_datetime = fake.date_time_between(start_date='-1y', end_date='-1d')
    # продолжительность звонка от 1 секунды до получаса
    new_start_datetime = start_datetime.replace(hour=23, minute=55, second=fake.random_int(min=0, max=59), microsecond=0)
    duration = random.randint(301, 1800)
    # дата и время окончания звонка
    end_datetime = new_start_datetime + datetime.timedelta(seconds=duration)
    all_entries[0][3] = new_start_datetime.isoformat()
    all_entries[0][4] = end_datetime.isoformat()
    # данный файл сохраняется
    filename = f"test_cdr/negative_cdr_through_midnight.txt"
    # вставляем по 10 записей в файл
    # первая - невалидная
    with open(filename, 'w') as f:
        for entry in all_entries:
            record = f"{entry[0]},{entry[1]},{entry[2]},{entry[3]},{entry[4]}"
            f.write(record + '\n')

# функция для создания негативных тестов
# некорректный номер
def generate_negative_incorrect_number():
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(1)
    # генерируем более длинный номер
    all_entries[0][2] = f'7{random.randint(900, 999)}{"".join(random.choices("0123456789", k=10))}'
    # данный файл сохраняется
    filename = f"test_cdr/negative_cdr_incorrect_number.txt"
    # вставляем по 10 записей в файл
    # первая - невалидная
    with open(filename, 'w') as f:
        for entry in all_entries:
            record = f"{entry[0]},{entry[1]},{entry[2]},{entry[3]},{entry[4]}"
            f.write(record + '\n')
num_positive_files = 5
# сгенерировать нужное количество файлов
generate_cdr_file(num_positive_files)
# генерируем файлы с негативными тестами
generate_negative_incorrect_datetime()
generate_negative_not_romashka()
generate_negative_call_myself()
generate_negative_incorrect_chronology()
generate_negative_through_midnight()
generate_negative_incorrect_number()