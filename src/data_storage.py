import pandas as pd
import os
from typing import List, Optional
from pathlib import Path

from src.models.data_models import MarketData
import src.config as config
from src.logger import get_logger

# Получаем логгер
logger = get_logger("data_storage")


class DataStorage:
    """Сервис для сохранения и загрузки данных из CSV файла"""
    
    def __init__(self, data_dir: Path = config.DATA_DIR, dataset_filename: str = config.DATASET_FILENAME):
        """
        Инициализирует сервис хранения данных
        
        Args:
            data_dir: Путь к директории для хранения данных
            dataset_filename: Имя файла для сохранения набора данных
        """
        self.data_dir = data_dir
        self.dataset_filepath = data_dir / dataset_filename
        
        # Создаем директорию для данных, если она не существует
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def save_to_csv(self, data_df: pd.DataFrame, filepath: Optional[Path] = None) -> bool:
        """
        Сохраняет DataFrame в CSV файл
        
        Args:
            data_df: DataFrame с данными для сохранения
            filepath: Путь для сохранения файла (если не указан, используется стандартный)
            
        Returns:
            True в случае успеха, False в случае ошибки
        """
        if data_df.empty:
            logger.warning("Пустой DataFrame, сохранение не выполнено")
            return False
        
        # Используем путь по умолчанию, если не указан другой
        filepath = filepath or self.dataset_filepath
        
        try:
            # Проверяем, существует ли файл
            file_exists = filepath.exists()
            
            # Если файл существует, добавляем данные без заголовка
            # Если файла нет, создаем его с заголовком
            data_df.to_csv(
                filepath,
                index=False,
                mode='a' if file_exists else 'w',
                header=not file_exists,
                float_format=f'%.{config.FLOAT_PRECISION}f'  # Формат для чисел с плавающей точкой
            )
            
            logger.info(f"Данные успешно сохранены в {filepath}, {len(data_df)} записей")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в CSV: {e}")
            return False
    
    def load_from_csv(self, filepath: Optional[Path] = None) -> pd.DataFrame:
        """
        Загружает данные из CSV файла
        
        Args:
            filepath: Путь к файлу для загрузки (если не указан, используется стандартный)
            
        Returns:
            DataFrame с загруженными данными (пустой в случае ошибки)
        """
        # Используем путь по умолчанию, если не указан другой
        filepath = filepath or self.dataset_filepath
        
        try:
            # Проверяем существование файла
            if not filepath.exists():
                logger.warning(f"Файл {filepath} не существует")
                return pd.DataFrame()
            
            # Загружаем данные из CSV
            df = pd.read_csv(filepath)
            
            logger.info(f"Данные успешно загружены из {filepath}, {len(df)} записей")
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из CSV: {e}")
            return pd.DataFrame()
    
    def save_market_data_list(self, market_data_list: List[MarketData], filepath: Optional[Path] = None) -> bool:
        """
        Сохраняет список объектов MarketData в CSV файл
        
        Args:
            market_data_list: Список объектов MarketData для сохранения
            filepath: Путь для сохранения (если не указан, используется стандартный)
            
        Returns:
            True в случае успеха, False в случае ошибки
        """
        if not market_data_list:
            logger.warning("Пустой список данных, сохранение не выполнено")
            return False
        
        # Преобразуем список объектов MarketData в плоские словари
        data = [item.to_dict() for item in market_data_list]
        
        # Создаем DataFrame
        df = pd.DataFrame(data)
        
        # Сохраняем в CSV
        return self.save_to_csv(df, filepath)
    
    def append_to_dataset(self, market_data_list: List[MarketData]) -> bool:
        """
        Добавляет новые данные к существующему набору данных
        
        Args:
            market_data_list: Список объектов MarketData для добавления
            
        Returns:
            True в случае успеха, False в случае ошибки
        """
        return self.save_market_data_list(market_data_list, self.dataset_filepath) 