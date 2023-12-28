from sqlalchemy import (Column,
                        String,
                        INT,
                        SMALLINT,
                        NUMERIC,
                        TIMESTAMP, select)
from sqlalchemy.orm import Session
from base import Base
from ex_db import get_db_inf0, data_from_db


# генерация нового класса таблицы на основе данных (в БД) о нем
def generate_table_db(db_info: dict) -> dict:
    list_classes = dict()
    for table in db_info:
        name_class = str(table).capitalize()
        fields_table = db_info.get(table)

        # подготовка словаря для инициализации класса
        attr_class_table = dict()

        # все аттрибуты класса = все поля одной таблицы
        for i in range(len(fields_table)):

            # подготовка ключевых слов для генерации колонки
            match fields_table[i][2]:
                case "integer":
                    field_type = INT
                case "smallint":
                    field_type = SMALLINT
                case "numeric":
                    field_type = NUMERIC
                case "timestamp without time zone":
                    field_type = TIMESTAMP
                case "character varying":
                    field_type = String

            field_nullable = False if fields_table[i][4] == "NO" else True
            field_pk = True if fields_table[i][1] == "id" else False

            # генерация одного аттрибута класса = поля таблицы
            attr_class_table[fields_table[i][1]] = Column(
                name=fields_table[i][1],
                type_=field_type,
                default=fields_table[i][3],
                nullable=field_nullable,
                primary_key=field_pk,)

        # добавление созданного класса в список таблиц БД
        list_classes[name_class] = type(name_class, (Base, ), attr_class_table)
        print(f"таблица БД {name_class} сгенерирована")
    return list_classes


db_info = get_db_inf0()
print(db_info)


# Base.metadata.create_all(bind=Base.engine)


list_classes_tables = generate_table_db(db_info)
print(list_classes_tables)
# for i in list_classes_tables:
#     print(i)
#     type(i)
Books = list_classes_tables.get("Books")
Users = list_classes_tables.get("Users")

# with Base.session() as session:
#     queryset = select(Books).limit(5)
#     objs = session.scalars(queryset)
#     result = objs.all()
# print(result)

# item = session.query(Food).filter(Food.type == x[0]).first()
# item = session.g
# item.quantity -= int(x[3])
# db.commit()

with Base.session() as session:
    result = session.query(Users)
    print(result)
