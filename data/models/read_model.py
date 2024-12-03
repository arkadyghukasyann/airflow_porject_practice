import dill

# Укажите путь к вашему pkl файлу
model_path = 'C:\\Users\\Slay Bitch\\PycharmProjects\\Airflow_practice33_6\\data\\models\\cars_pipe.pkl'

# Загрузка модели
with open(model_path, 'rb') as f:
    model = dill.load(f)

# Вывод информации о модели
print(model)