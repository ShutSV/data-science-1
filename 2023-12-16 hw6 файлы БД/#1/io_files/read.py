import csv
import openpyxl


# Чтение текстового файла и его свойств
def read_txt(file_path: str) -> tuple:
    try:
        with open(file_path, "r") as file:
            content = file.readlines()
            qnty_row = len(content)
            return content, qnty_row
    except Exception as e:
        print("Ощибка чтения файла", e)


# Чтение файла CSV и его свойств
def read_csv(file_path: str) -> tuple:
    try:
        with open(file_path, "r") as file:
            content = csv.reader(file)
            out_content = []
            for _ in content:
                out_content.append(_)
            qnty_row = len(out_content)
        return out_content, qnty_row
    except Exception as e:
        print("Ощибка чтения файла", e)


# Чтение файла XLS и его свойств
def read_xls(file_path: str) -> tuple:
    try:
        workbook = openpyxl.load_workbook(file_path)
        sheet = workbook.active
        out_content = []
        for _ in sheet.iter_rows(values_only=True):
            out_content.append(_)
        qnty_row = len(out_content)
        return out_content, qnty_row
    except Exception as e:
        print("Ощибка чтения файла", e)


# выбор функции в зависимости от типа файла
def read(file_path: str) -> tuple:
    match file_path[-4:]:
        case "xlsx":
            obj = read_xls(file_path)
        case ".csv":
            obj = read_csv(file_path)
        case _:
            obj = read_txt(file_path)
    return obj
