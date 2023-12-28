from psycopg2 import connect as connect


# функция проверки подключения к БД
def _get_db_session():
    try:
        conn = connect(
            dbname='admin',
            user='admin',
            password='admin',
            host='0.0.0.0',
            port=5432)
        print("\nСоединение с БД установлено", conn, "\n")
        return conn
    except Exception as e:
        print('БД не доступна. Проверьте Docker', e)
        return False


# функция выполнения запроса к БД c получением ответа
def query_get(conn, query: str) -> dict:
    with conn.cursor() as curs:
        curs.execute(query)
        result = curs.fetchall()
        return result


# функция выполнения запроса к БД не предполагающая получения ответа
def query_set(conn, query: str):
    with conn.cursor() as curs:
        curs.execute(query)
        conn.commit()
        print(f"выполнен запрос к БД: '{query}' \n")


# функция построчного вывода на экран результатов запроса
def query_n_print(conn, query_read: str):
    print(f"Выполнен следующий запрос к БД: '{query_read}'")
    print("Результат запроса: ")
    result = query_get(conn, query_read)
    for row in result:
        print(row)


# проверкв подключения к БД
dbconnect = _get_db_session()
if dbconnect is False:
    print("БД не подключена. \nПрограмма завершена")
else:
    conn = dbconnect

    # создание таблицы users в БД (если не существует)
    query_create_table = """create table if not exists users(
            id serial primary key,
            name varchar not null,
            age int,
            gender varchar,
            nationality varchar);"""
    query_set(conn, query_create_table)

    # получение всех записей из таблицы users
    query_read = """SELECT * FROM users"""
    query_n_print(conn, query_read)

    # добавление записей в таблицу users. Вариант №1
    query_add_table = """INSERT INTO users 
    (name, age, gender, nationality)
    VALUES
      ('James', 25, 'male', 'USA'),
      ('Leila', 32, 'female', 'France'),
      ('Brigitte', 35, 'female', 'England'),
      ('Mike', 40, 'male', 'Denmark'),
      ('Elizabeth', 21, 'female', 'Canada');"""
    query_set(conn, query_add_table)

    # выполнение произвольного запроса к БД на запись/изменение БД
    query_save = input("Введите запрос SQL на создание/изменение БД: ")
    try:
        query_set(conn, query_save)
    except Exception as e:
        print("запрос в БД не выполнен:", e)

    # выполнение произвольного запроса на получение данных из БД
    query_read = input("Введите запрос SQL на получение данных из БД: ")
    try:
        query_n_print(conn, query_read)
    except Exception as e:
        print("запрос в БД не выполнен:", e)

    # закрытие подключения к БД
    conn.close()
    print("-- программа завершена --")
