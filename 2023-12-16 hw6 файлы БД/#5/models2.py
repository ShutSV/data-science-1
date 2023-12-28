from ex_db import get_db_info, get_data_from_db, _get_db_session


db_info = get_db_info()
print(db_info)

data_from_db = get_data_from_db()
for i in data_from_db:
    for j in i:
        print(j)
