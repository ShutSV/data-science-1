import csv
import openpyxl


# Запись в текстовый файл
def save_txt(file_path: str, obj: list):
    add_text = "\n" + ",".join(obj[1:])
    try:
        with open(file_path, "a") as file:
            file.write(add_text)
            print("\nФайл обновлен")
    except Exception as e:
        print("Ощибка записи файла", e)


# Запись в файл CSV
def save_csv(file_path: str, obj: list):
    try:
        with open(file_path, "a") as file:
            output = csv.writer(file)
            output.writerow(obj)
            print("\nФайл обновлен")
    except Exception as e:
        print("Ощибка записи файла", e)


# Запись в файл XLS
def save_xls(file_path: str, obj: list):
    add_data_list = obj[1:]
    num_row = obj[0]

    try:
        workbook = openpyxl.load_workbook(file_path)
        sheet = workbook.active
        for i in range(1, len(add_data_list)):
            cell_value = add_data_list[i - 1]
            if cell_value.isdigit():
                cell_value = int(cell_value)
            sheet.cell(row=num_row, column=i, value=cell_value)
        workbook.save(file_path)
        print("\nФайл обновлен")
    except Exception as e:
        print("Ощибка записи файла", e)


# выбор функции в зависимости от типа файла
def save(file_path: str, obj):
    match file_path[-4:]:
        case "xlsx":
            save_xls(file_path, obj)
        case ".csv":
            save_csv(file_path, obj)
        case _:
            save_txt(file_path, obj)
