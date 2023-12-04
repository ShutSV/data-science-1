import pandas as pd

import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import LabelEncoder

#
# Bohanec,Marko. (1997). Car Evaluation. UCI Machine Learning Repository. https://doi.org/10.24432/C5JP48.
#

# импорт датасета в DataFrame
df = pd.read_csv(
    'car.csv',
    header=None,
    names=["buying", "maint", "doors", "persons", "lug_boot", "safety", "class"],
    index_col=False,
    sep=','
)

# Базовые статистики
print("-" * 20, "Базовые статистики", "-" * 20)
print(df.describe())
print()

# Проверка на пропущенные значения
print("-" * 20, "Проверка на пропущенные значения", "-" * 20)
print(df.isnull().sum())
print()

# Первичный анализ структуры данных
print("-" * 20, "Первичный анализ структуры данных", "-" * 20)
print(df.dtypes)
print("-" * 20)
print(df.info())
print()

# Преобразование категориальных данных в числовые (Create an instance of the encoder)
encoder = LabelEncoder()

# Fit and transform the names=["buying", "maint", "doors", "persons", "lug_boot", "safety"] column
df['encoded_buying'] = encoder.fit_transform(df['buying'])
df['encoded_maint'] = encoder.fit_transform(df['maint'])
df['encoded_doors'] = encoder.fit_transform(df['doors'])
df['encoded_persons'] = encoder.fit_transform(df['persons'])
df['encoded_lug_boot'] = encoder.fit_transform(df['lug_boot'])
df['encoded_safety'] = encoder.fit_transform(df['safety'])

# Повторный анализ структуры данных после преобразования типов
print("-" * 20, "Повторный анализ структуры данных", "-" * 20)
print(df.dtypes)
print("-" * 20)
print(df.info())
print()


# Для красивой карты корреляций - Установка стиля Seaborn
sns.set(style="white")

# Расчет корреляционной матрицы
corr = df.iloc[:, 7:].corr()

# Маска для отображения только нижней треугольной части матрицы (опционально)
mask = np.triu(np.ones_like(corr, dtype=bool))

# Настройка цветовой палитры
cmap = sns.diverging_palette(230, 20, as_cmap=True)

# Создание тепловой карты
plt.figure(figsize=(10, 8))
sns.heatmap(corr, mask=mask, cmap=cmap, vmax=.3, center=0,
            square=True, linewidths=.5, cbar_kws={"shrink": .5}, annot=True)

# Добавление заголовка
plt.title('Тепловая карта корреляций', fontsize=20)

# Красивая тепловая диаграмма - Показать график
plt.show()

# Установка стиля Seaborn для красивых графиков
sns.set(style="whitegrid")

# Создание гистограмм для каждой числовой переменной
df.iloc[:, 7:].hist(bins=20, figsize=(15, 10), color='skyblue', edgecolor='black')

# Добавление названий для каждого графика и осей
for ax in plt.gcf().get_axes():
    ax.set_xlabel('Значение')
    ax.set_ylabel('Частота')
    ax.set_title(ax.get_title().replace('class', 'Класс'))

# Регулировка макета для предотвращения наложения подписей
plt.tight_layout()

# Показать график
plt.show()

# Парные диаграммы рассеяния
sns.pairplot(df, hue="class")
plt.show()


# Разделение данных на признаки (X) и целевую переменную (y)
X = df.iloc[:, 7:]
y = df['class']

# Разделение на обучающую и тестовую выборки
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Стандартизация данных
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Создание и обучение моделей
log_reg = LogisticRegression()
log_reg.fit(X_train_scaled, y_train)

rf = RandomForestClassifier()
rf.fit(X_train_scaled, y_train)

# Предсказание на тестовых данных
y_pred_log_reg = log_reg.predict(X_test_scaled)
print("*-" * 50)
print("y_pred_log_reg=\n", y_pred_log_reg)
print("*-" * 50)

y_pred_rf = rf.predict(X_test_scaled)

# Оценка моделей
accuracy_log_reg = accuracy_score(y_test, y_pred_log_reg)
accuracy_rf = accuracy_score(y_test, y_pred_rf)
report_log_reg = classification_report(y_test, y_pred_log_reg)
report_rf = classification_report(y_test, y_pred_rf)

# Предсказания логистической регрессии
y_pred_log_reg = log_reg.predict(X_test_scaled)
print("Accuracy of Logistic Regression: ", accuracy_score(y_test, y_pred_log_reg))
print()

# Предсказания случайного леса
y_pred_rf = rf.predict(X_test_scaled)
print("Accuracy of Random Forest: ", accuracy_score(y_test, y_pred_rf))
print()

print("*" * 50)
print("accuracy_log_reg=\n", accuracy_log_reg)
print("*" * 50)
print("accuracy_rf=\n", accuracy_rf)
print("*" * 50)
print("report_log_reg=\n", report_log_reg)
print("*" * 50)
print("report_rf=\n", report_rf)
print("*" * 50)
print("y_pred_log_reg =\n", y_pred_log_reg )
