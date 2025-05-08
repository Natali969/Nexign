import pytest
import psycopg2
# Раскладывает CDR-файл на записи, которые записывваются в список
def get_amount_calls(db_connection_brt, request):
    try:
        cur = db_connection_brt.cursor()
        cur.execute(request)
        cdrs_brt = [row for row in cur.fetchall()]
        start_amount_calls = len(cdrs_brt)
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    return start_amount_calls

# Раскладывает CDR-файл на записи, которые записывваются в список
def get_dates_cdr(db_connection_brt):
    try:
        cur = db_connection_brt.cursor()
        # получим список из даты и времен начала звонка
        # для последующей проверки хронологии
        cur.execute("SELECT start_date_time FROM (SELECT * FROM cdrs ORDER BY id DESC LIMIT 10) AS cdr ORDER BY id")
        dates_cdr = [row for row in cur.fetchall()]
    except psycopg2.Error as e:
        pytest.fail(f"Ошибка при запросе к PostgreSQL: {e}")
    except Exception as e:
        pytest.fail(f"Произошла ошибка: {e}")
    return dates_cdr