import os
import csv
import matplotlib.pyplot as plt

# загрузка данных из файла CSV, без перевода в DataFrame
file_path = input("Введите имя файла с расширением: ")
if not os.path.exists(file_path):
    print("Такой файл не существует")

# X и Y, выбор колонок в данных
x = []
y = []
num_x = 3
num_y = 7

# попвтка получить данные
try:
    with open(file_path, "r") as file:
        content = csv.reader(file)
        out = []
        for _ in content:
            out.append(_)
except Exception as e:
    print("Ошибка чтения из файла", e)

# превращение данных в координаты графика
# и ограничение графика первыми 50 строками (отсекая заголовки)
for i in range(1, 51):
    x.append(out[i][num_x])
    y.append(out[i][num_y])

# настройки графика
plt.plot(x, y)
plt.title(f'Зависимость значений столбца {num_y} от значений столбца {num_x}')
plt.xlabel(f'X (столбец {out[0][num_x]})')
plt.ylabel(f'Y (столбец {out[0][num_y]})')

# вывод графика
plt.show()
