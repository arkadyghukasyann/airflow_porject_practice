import os
import dill
import pandas as pd
import logging
import json  # Импортируем модуль json
os.environ['PROJECT_PATH'] = r'C:\Users\Slay Bitch\PycharmProjects\Airflow_practice33_6\airflow_hw'
# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Указываем базовый путь к проекту
project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
logging.info(f"Current project_path: {project_path}")

# Пути к папкам
data_dir = os.path.join(project_path, 'data')
model_dir = os.path.join(data_dir, 'models')
test_dir = os.path.join(data_dir, 'test')
predictions_dir = os.path.join(data_dir, 'predictions')

def load_model(model_path: str):
    with open(model_path, 'rb') as f:
        return dill.load(f)

def check_required_columns(test_data: pd.DataFrame, required_columns: list):
    missing_columns = [col for col in required_columns if col not in test_data.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in test data: {', '.join(missing_columns)}")

def predict():
    # Проверка существования директорий
    if not os.path.exists(model_dir):
        raise FileNotFoundError(f"Model directory does not exist: {model_dir}")
    if not os.path.exists(test_dir):
        raise FileNotFoundError(f"Test directory does not exist: {test_dir}")

    # Загружаем модель
    model_files = sorted([f for f in os.listdir(model_dir) if f.endswith('.pkl')])
    if not model_files:
        raise FileNotFoundError(f"No models found in {model_dir}")
    model_path = os.path.join(model_dir, model_files[-1])
    model = load_model(model_path)

    # Определяем необходимые колонки
    required_columns = ['id', 'year', 'odometer', 'lat', 'long',
                        'url', 'region', 'manufacturer', 'model',
                        'fuel', 'title_status', 'transmission',
                        'image_url', 'description', 'state',
                        'posting_date']

    # Загружаем тестовые данные и делаем предсказания
    test_files = [os.path.join(test_dir, f) for f in os.listdir(test_dir) if f.endswith('.json')]
    if not test_files:
        raise FileNotFoundError(f"No test files found in {test_dir}")

    predictions = []
    for test_file in test_files:
        try:
            logging.info(f"Loading test data from: {test_file}")
            with open(test_file, 'r') as f:
                test_data_json = json.load(f)  # Загружаем JSON-данные
            test_data = pd.DataFrame([test_data_json])  # Преобразуем в DataFrame

            # Проверка на наличие необходимых колонок
            check_required_columns(test_data, required_columns)

            # Предсказания
            test_data['prediction'] = model.predict(test_data)
            predictions.append(test_data)

        except ValueError as ve:
            logging.error(f"ValueError: {ve} for file: {test_file}")
        except Exception as e:
            logging.error(f"Error processing file {test_file}: {e}")

    if predictions:
        predictions_df = pd.concat(predictions, ignore_index=True)
        os.makedirs(predictions_dir, exist_ok=True)
        predictions_path = os.path.join(predictions_dir, 'predictions.csv')
        predictions_df.to_csv(predictions_path, index=False)
        logging.info(f"Predictions saved to {predictions_path}")
    else:
        logging.warning("No predictions were made.")

if __name__ == '__main__':
    predict()