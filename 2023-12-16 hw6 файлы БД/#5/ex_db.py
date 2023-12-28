import os
from psycopg2 import connect as connect
import datetime
# from contextlib import contextmanager


# функция проверки подключения к БД
def _get_db_session():
    try:
        connection = connect(
            dbname='admin',
            user='admin',
            password='admin',
            host='0.0.0.0',
            port=5432)
        print("\nСоединение с БД установлено", connection, "\n")
        return connection

    except Exception as e:
        print('БД не доступна. Проверьте Docker', e)
        return False


    # @contextmanager
    # def session_scope():
    #     """Provides a transactional scope around a series of operations."""
    #     session = DBSession()
    #     try:
    #         yield session
    #         session.commit()
    #     except Exception as e:
    #         session.rollback()
    #         raise e
    #     finally:
    #         session.close()




# получение списка таблиц
def get_list_tables_from_db() -> list:
    conn = _get_db_session()
    cursor = conn.cursor()

    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema='public'""")
    tables = cursor.fetchall()
    return tables


# получение инф из существующей db
def get_db_info() -> dict:
    conn = _get_db_session()
    cursor = conn.cursor()

    # подготовка логов
    current_time = datetime.datetime.now()
    add_text = "\n\n" + str(current_time) + "\nПолучение структуры БД"

    # получение списка таблиц
    tables = get_list_tables_from_db()

    # для каждой таблицы получение детальной информации о ее полях
    result = dict()
    for i in tables:
        cursor.execute(f"""
    SELECT
        ordinal_position,
        column_name,
        data_type,
        column_default,
        is_nullable,
        character_maximum_length,
        numeric_precision
    FROM information_schema.columns
    WHERE table_name = '{i[0]}'
    ORDER BY ordinal_position
    """)
        fields_table_info = cursor.fetchall()

        # сбор результатов в словарь и лог информации в файл
        result[str(i[0])] = fields_table_info
        add_text += "\n" + str(i[0])
        for j in fields_table_info:
            add_text += "\n" + str(j)

    conn.close()
    save_to_log(add_text)
    return result


# запись логов
def save_to_log(add_text):
    file_path = "log_db_info.txt"
    if not os.path.exists(file_path):
        try:
            with open(file_path, "w") as file:
                file.write("")
                print("Файл логов создан")
        except Exception as e:
            print("Файл логов не создан", e)

    try:
        with open(file_path, "a") as file:
            file.write(add_text)
            print("\nФайл логов обновлен")
    except Exception as e:
        print("Ощибка записи лога в файл", e)


def get_data_from_db() -> list:
    conn = _get_db_session()
    cursor = conn.cursor()
    db_data = []

    tables = get_list_tables_from_db()
    for table in tables:
        cursor.execute(f"""SELECT * FROM {table[0]}""")
        data_table = cursor.fetchall()
        db_data.append(data_table)

    return db_data
