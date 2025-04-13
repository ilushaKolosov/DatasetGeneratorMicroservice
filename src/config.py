import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from typing import List

# Загрузка переменных окружения из .env файла
load_dotenv()

# Конфигурация логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Настройки API биржи
EXCHANGE = os.getenv("EXCHANGE", "binance")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# Настройки сбора данных
DATA_COLLECTION_INTERVAL = int(os.getenv("DATA_COLLECTION_INTERVAL", 3600))
SYMBOLS = os.getenv("SYMBOLS", "BTC/USDT,ETH/USDT").split(",")
TIMEFRAMES = os.getenv("TIMEFRAMES", "1h,4h,1d").split(",")
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", 365))

# Пути к данным
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATASET_FILENAME = os.getenv("DATASET_FILENAME", "crypto_dataset.csv")
DATASET_PATH = DATA_DIR / DATASET_FILENAME

# Сетевые настройки
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 30000))
RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true"

# Настройки датасета
FLOAT_PRECISION = int(os.getenv("FLOAT_PRECISION", 8))


# Создаем директорию для данных, если она не существует
DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_log_level() -> int:
    """Преобразует строковое представление уровня логирования в константу logging"""
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    return levels.get(LOG_LEVEL.upper(), logging.INFO)