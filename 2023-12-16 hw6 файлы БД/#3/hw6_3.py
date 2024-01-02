from psycopg2 import connect as connect
from datetime import datetime
import numpy as np

# на примере БД dvdrental


# проверкв подключения к БД
def _get_db_session(dbname, user, password):
    try:
        conn = connect(
            dbname=dbname, user=user, password=password, host="0.0.0.0", port=5432
        )
        print("\nСоединение с БД установлено", conn, "\n")
        return conn
    except Exception as e:
        print("БД не доступна. Проверьте Docker", e)
        return False


# функция выполнения запроса к БД c получением ответа
def query_get(conn, query: str) -> dict:
    with conn.cursor() as curs:
        curs.execute(query)
        result = curs.fetchall()
        return result


# функция построчного вывода на экран результатов запроса
def query_n_print(conn, query_read: str):
    print("\n", f"Выполнен запрос к БД - '{query_read}'", ":\n", "-" * 50)
    result = query_get(conn, query_read)
    for row in result:
        print(row)


# функция выполнения запроса к БД не предполагающая получения ответа
def query_set(conn, query: str):
    with conn.cursor() as curs:
        curs.execute(query)
        conn.commit()
        print(f"выполнен запрос к БД: '{query}' \n")


dbname = "dvdrental"
user = "admin"
password = "admin"
conn = _get_db_session(dbname, user, password)

# простые запросы типа query = """select * from category"""
query_n_print(conn, """select * from category limit 20""")
query_n_print(conn, """select * from film_category limit 20""")
query_n_print(conn, """select * from customer where first_name = 'John'""")


# SQL-запрос с использованием подзапросов (subqueries) для выбора данных из нескольких таблиц.


# JOIN-операции для объединения данных из двух таблиц.
# Объединение двух таблиц по полю "category_id", с выводом только тех строк, которые совпадают в обеих таблицах
query = """SELECT
    c.category_id id_cat_cat,
    c.name name_cat,
    f.film_id id_film,
    f.category_id id_cat_film
FROM
    category c
INNER JOIN film_category f ON c.category_id = f.category_id
ORDER BY id_film
LIMIT 50 OFFSET 50"""
query_n_print(conn, query)

# Объединение двух таблиц по полю "category_id", с выводом всех строк из таблицы category (первой в формуле, т.е. LEFT),
# и тех строк из таблицы film_category, в которых есть совпадения по полю "category_id"
query = """SELECT
    c.category_id id_cat_cat,
    c.name name_cat,
    f.film_id id_film,
    f.category_id id_cat_film
FROM
    category c
LEFT JOIN film_category f ON c.category_id = f.category_id
ORDER BY id_cat_cat
LIMIT 20"""
query_n_print(conn, query)

# Объединение двух таблиц по полю "category_id", с выводом всех строк из таблицы category (первой в формуле, т.е. LEFT),
# которых нет в таблице film_category, т.е. не имеющих совпадений по полю "category_id"
query = """SELECT
    c.category_id id_cat_cat,
    c.name name_cat,
    f.film_id id_film,
    f.category_id id_cat_film
FROM
    category c
LEFT JOIN film_category f ON c.category_id = f.category_id
WHERE f.film_id is NULL
"""
query_n_print(conn, query)

# Полное объединение двух таблиц по полю "category_id", с выводом всех строк обеих таблиц. При отсутствии совпадений
# поля заполняются значениями NULL
# в формуле необязательно использование ключевого слова OUTER
query = """SELECT
    c.category_id id_cat_cat,
    c.name name_cat,
    f.film_id id_film,
    f.category_id id_cat_film
FROM
    category c
FULL OUTER JOIN film_category f ON c.category_id = f.category_id
"""
query_n_print(conn, query)

# Вывод уникальных строк двух таблиц, т.е. только тех строк двух таблиц,
# которые не имеют совпадений по полю "category_id"
# в формуле не используется ключевое слова OUTER
query = """SELECT
    c.category_id id_cat_cat,
    c.name name_cat,
    f.film_id id_film,
    f.category_id id_cat_film
FROM
    category c
FULL JOIN film_category f ON c.category_id = f.category_id
WHERE c.category_id is NULL OR f.category_id is NULL
"""
query_n_print(conn, query)

# Объединение ТРЕХ таблиц по полю "film_id" (поле есть во всех таблицах)
# с выводом только тех строк, которые совпадают во всех таблицах
query = """SELECT
    i.film_id id_film_inventory,
    i.store_id id_store_inventory,
    f.title title_film,
    fc.category_id id_cat_film
FROM
    film f
INNER JOIN film_category fc ON f.film_id = fc.film_id
INNER JOIN inventory i ON fc.film_id = i.film_id
LIMIT 100"""
query_n_print(conn, query)


# Создание индекса на одной из таблиц и измерение производительности запроса до и после создания индекса.
# --- измерять производительность лучше поочередно до и после создания индекса во избежания влияния кэширования
# Сперва проверка времени запроса c индексом, Добавление индекса на столбец "length"
# затем - после удаления индекса

# пакетная проверка времени запросов, перемежаемых иными
query_set(conn, """CREATE INDEX IF NOT EXISTS index_length ON film (length)""")
time_with_index_array = np.array([])

for i in range(100):
    for j in range(100):
        start_time = datetime.now()
        query_get(conn, f"""SELECT * FROM film WHERE length = {80 + j}""")
        stop_time = datetime.now()
        time_with_index_array = np.append(
            time_with_index_array,
            [stop_time - start_time],
        )
        query_get(conn, f"""SELECT * FROM film LIMIT {1 + j}""")

query_set(conn, """DROP INDEX IF EXISTS index_length""")
time_witout_index_array = np.array([])

for i in range(100):
    for j in range(100):
        start_time = datetime.now()
        query_get(conn, f"""SELECT * FROM film WHERE length = {80 + j}""")
        stop_time = datetime.now()
        time_witout_index_array = np.append(
            time_witout_index_array,
            [stop_time - start_time],
        )
        query_get(conn, f"""SELECT * FROM film LIMIT {1 + j}""")

print(
    f"\nСреднее время {len(time_witout_index_array)} запросов без индекса = {time_witout_index_array.mean()}",
    f"\nСреднее время {len(time_with_index_array)} запросов c индексом = {time_with_index_array.mean()}",
)


# SQL-запрос с использованием оконных функций (window functions) для анализа данных внутри окна.


# применение операторов DDL (CREATE, ALTER, DROP) для управления структурой базы данных.


# закрытие подключения к БД
conn.close()
print("-- программа завершена --")
