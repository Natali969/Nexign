import psycopg2
from psycopg2 import Error
# Раскладывает CDR-файл на записи, которые записывваются в список
def is_calls_intersect(all_entries, entries, is_call_midnight):
    # проверить, что свежесгенированный звонок(-ки) не перескается
    # с уже существующими
    calls_intersect = False
    # выбираем из свежих записей абонентов
    subscribers_called = [entries[0][1], entries[0][2]]
    for i in range(len(all_entries)):
        if entries[0][1] == entries[0][2]:
            calls_intersect = True
            return calls_intersect
        else:
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
                    return calls_intersect
    return calls_intersect
