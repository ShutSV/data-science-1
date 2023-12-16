import csv
import openpyxl


# создание текстового файла
def create_txt(file_path: str):
    try:
        with open(file_path, "w") as file:
            file.write("")
            print("Файл создан")
    except Exception as e:
        print("Файл не создан", e)


# создание файла csv
def create_csv(file_path: str):
    try:
        with open(file_path, "w") as file:
            csv.writer(file)
            print("Файл создан")
    except Exception as e:
        print("Файл не создан", e)


# создание файла xls
def create_xls(file_path: str):
    try:
        workbook = openpyxl.Workbook()
        workbook.save(file_path)
        print("Файл создан")
    except Exception as e:
        print("Файл не создан", e)


# выбор функции в зависимости от типа файла
def create(file_path: str):
    match file_path[-4:]:
        case "xlsx":
            create_xls(file_path)
        case ".csv":
            create_csv(file_path)
        case _:
            create_txt(file_path)
