import os
from io_files import create, read, save


# запрос имени файла
file_path = input("Введите имя файла с расширением: ")

# проверка существования файла
if not os.path.exists(file_path):
    create(file_path)
else:
    print("Такой файл существует")


# подтверждение действий
def confirm(ask: str) -> bool:
    usr_choice = input(f"{ask}(y/n): ")
    while usr_choice not in ["Y", "y", "N", "n"]:
        usr_choice = input(f"{ask} (y/n): ")
    if usr_choice in ["Y", "y"]:
        return True
    return False


# получение свойств файла, включая его содержимое
input_file = read(file_path)
content_file = input_file[0]
inf_file = input_file[1]
print(f"Информация о файле: в файле строк/записей: {inf_file}")
if confirm("Вывести содержимое файла на экран?"):
    for row in content_file:
        print(row)

# добавление информации в файл
if confirm("Дополнить содержимое файла?"):
    add_object = input("Введите список значений через запятую: ").split(", ")
    save(file_path, [inf_file + 1] + add_object)

print("Программа завершена")
