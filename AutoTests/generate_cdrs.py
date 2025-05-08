import random
import datetime
from faker import Faker
from psycopg2 import Error
from modulesCDR.db_connection import db_connection_brt
from modulesCDR.work_db import get_subscribers_list


fake = Faker('ru_RU')
outgoing_call = '01'
ingoing_call = '02'
subscribers_list = get_subscribers_list()
# генерация записи по абоненту
def generate_cdr_entry():
    # список номеров абонентов "Ромашки" из БД
    subscribers_list = get_subscribers_list()
    # тип вызова: исходящий или входящий
    call_type = random.choice([outgoing_call, ingoing_call])

    # номер абонента должен выбираться из БД запросом SELECT,
    # а дальше - рандомный выбор в коде

    # временная замена - список номеров абонентов
    # обслуживаемый абонент выбирается из БД
    our_subscriber = random.choice(subscribers_list)
    subscribers_list.remove(our_subscriber)
    # с кем осуществлялась связь - генерируется случайно
    other_subscriber = f'7{random.randint(900, 999)}{"".join(random.choices("0123456789", k=7))}'

    # если разговор был между двумя абонентами оператора "ромашка"
    double_call = True if other_subscriber in subscribers_list else False

    # генерируется дата и время начала звонка
    start_datetime = fake.date_time_between(start_date='-1y', end_date='-1d')
    # продолжительность звонка от 1 секунды до получаса
    duration = random.randint(1, 72000)

    # дата и время окончания звонка
    end_datetime = start_datetime + datetime.timedelta(seconds=duration)

    # если разговор пересёк границу полуночи
    midnight_call = True if start_datetime.date() != end_datetime.date() else False

    # генерируем записи
    entries = []
    # если звонок пересёк границу полуночи,
    # то его нужно разделить
    if midnight_call:
        midnight = datetime.datetime(start_datetime.year, start_datetime.month, start_datetime.day, 23, 59, 59)
        after_midnight = datetime.datetime(end_datetime.year, end_datetime.month, end_datetime.day, 0, 0, 0)
        # если звонок осуществлялся между двумя абонентами "Ромашки"
        # нужно сгенерировать четыре звонковых записи
        # одна - исходящий вызов абонента А до 23:59:59
        # вторая - входящий вызов абоненту Б до 23:59:59
        # третья - исходящий вызов абонента А после 00:00:00
        # четвёртая - входящий вызов абоненту Б после 00:00:00
        if double_call:
            call_type_anti = ingoing_call if call_type == outgoing_call else outgoing_call
            entries.append([call_type, our_subscriber, other_subscriber,
                            start_datetime.isoformat(), midnight.isoformat()])
            entries.append([call_type_anti,other_subscriber, our_subscriber,
                            start_datetime.isoformat(), midnight.isoformat()])
            
            entries.append([call_type, our_subscriber, other_subscriber,
                            after_midnight.isoformat(), end_datetime.isoformat()])
            entries.append([call_type_anti,other_subscriber, our_subscriber,
                            after_midnight.isoformat(), end_datetime.isoformat()])
        # если звонок осуществлялся между абонентом Ромашки и
        # абонентов другого оператора, то нужно просто разбить
        # этот звонок на до полнуночи и после неё
        else:
            entries.append([call_type, our_subscriber, other_subscriber,
                            start_datetime.isoformat(), midnight.isoformat()])
            entries.append([call_type, our_subscriber, other_subscriber,
                            after_midnight.isoformat(), end_datetime.isoformat()])
            
    else:
        # если звонок осуществлялся между двумя абонентами "Ромашки"
        # нужно сгенерировать две звонковых записи
        # одна - исходящий вызов абонента А
        # вторая - входящий вызов абоненту Б
        if double_call:
            entries.append([call_type, our_subscriber, other_subscriber,
                            start_datetime.isoformat(), end_datetime.isoformat()])
            
            call_type_anti = ingoing_call if call_type == outgoing_call else outgoing_call

            entries.append([call_type_anti, other_subscriber, our_subscriber,
                            start_datetime.isoformat(), end_datetime.isoformat()])
        else:
            entries.append([call_type, our_subscriber, other_subscriber,
                            start_datetime.isoformat(), end_datetime.isoformat()])
            
    return entries, midnight_call

def generate_all_entries(num_cdr, num_records = 10):
    # отсортированные записи о звонках для всех n файлов
    all_entries = []
    while len(all_entries) < num_cdr * num_records:
        entries, is_call_midnight = generate_cdr_entry()
        # проверить, что свежесгенированный звонок(-ки) не перескается
        # с уже существующими
        calls_intersect = False
        # выбираем из свежих записей абонентов
        subscribers_called = [entries[0][1], entries[0][2]]
        for i in range(len(all_entries)):
            # если один из абонентов раннее был записан в файле с звонками
            if all_entries[i][1] in subscribers_called or all_entries[i][2] in subscribers_called:
                # если звонок пересекает границу полуночи
                # необходимо брать границы двух звонков -
                # до полуночи и после неё
                if is_call_midnight:
                    # определим дату и время начала и
                    # дату и время окончания звонка
                    start_datetime = entries[0][3]
                    # если не было двойного звонка между абонентами Ромашки
                    # то звонок после полуночи находится на 2 месте
                    if len(entries) == 2:
                        end_datetime = entries[1][4]
                    # если двойной звонон между абонентами Ромашки был
                    # то звонок после полуночи находится на 3 месте
                    else:
                        end_datetime = entries[2][4]
                # если звонок не пересекает границу полуночи,
                # то данные о его начале и конце берутся из первой записи
                else:
                    start_datetime = entries[0][3]
                    end_datetime = entries[0][4]
                # если звонки не перескаются
                if end_datetime < all_entries[i][3] or start_datetime > all_entries[i][4]:
                    continue
                # если пересекаются, то проверка прекращается,
                # а сгенерированный звонок не добавляется
                else:
                    calls_intersect = True
                    break
        # если звонок не пересекается с уже существующими
        if not calls_intersect:
            for entry in entries:
                all_entries.append(entry)
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
def generate_negative_incorrect_datetime(num_negative_cdr=1):
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(num_negative_cdr)
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
def generate_negative_not_romashka(num_negative_cdr=1):
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(num_negative_cdr)

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
def generate_negative_call_myself(num_negative_cdr=1):
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(num_negative_cdr)

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
def generate_negative_incorrect_chronology(num_negative_cdr=1):
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(num_negative_cdr)

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
def generate_negative_through_midnight(num_negative_cdr=1):
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(num_negative_cdr)

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
def generate_negative_incorrect_number(num_negative_cdr=1):
    # отсортированные записи о звонках для всех n файлов
    all_entries = generate_all_entries(num_negative_cdr)
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
