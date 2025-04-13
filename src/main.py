import time
import schedule
from datetime import datetime
from typing import List, Dict, Any
import tqdm
import sys
import os
from pathlib import Path

# Добавляем родительскую директорию в путь для импортов
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.append(str(parent_dir))

from src.exchange_service import ExchangeService
from src.data_processor import DataProcessor
from src.data_storage import DataStorage
from src.models.data_models import MarketData
import src.config as config
from src.logger import get_logger

# Получаем логгер
logger = get_logger("main")


class DataCollector:
    """Основной класс микросервиса для сбора данных"""
    
    def __init__(self):
        """Инициализирует микросервис сбора данных"""
        logger.info("Инициализация микросервиса сбора данных")
        
        # Инициализация сервисов
        self.exchange_service = ExchangeService()
        self.data_processor = DataProcessor()
        self.data_storage = DataStorage()
        
        # Загружаем настройки
        self.symbols = config.SYMBOLS
        self.timeframes = config.TIMEFRAMES
        self.collection_interval = config.DATA_COLLECTION_INTERVAL
        
        # Рассчитываем общее количество запросов для прогресс-бара
        self.total_requests = len(self.symbols) * len(self.timeframes)
        
        logger.info(f"Микросервис настроен для сбора данных по {len(self.symbols)} символам и {len(self.timeframes)} таймфреймам")
    
    def collect_data(self) -> bool:
        """
        Выполняет сбор данных с биржи, обработку и сохранение
        
        Returns:
            True в случае успеха, False в случае ошибки
        """
        try:
            logger.info(f"Начало сбора данных: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Начало сбора данных по {len(self.symbols)} символам и {len(self.timeframes)} таймфреймам")
            
            # Создаем счетчик для прогресс-бара
            total_pairs = len(self.symbols) * len(self.timeframes)
            progress_bar = tqdm.tqdm(total=total_pairs, desc="Сбор данных", unit="пары")
            
            # Собираем данные для всех символов и таймфреймов
            all_market_data = []
            
            for symbol in self.symbols:
                for timeframe in self.timeframes:
                    # Обновляем описание прогресс-бара
                    progress_bar.set_description(f"Сбор {symbol} {timeframe}")
                    
                    # Получаем исторические данные
                    logger.info(f"Получение данных для {symbol} на таймфрейме {timeframe}")
                    ohlcv_data = self.exchange_service.fetch_historical_ohlcv(
                        symbol=symbol,
                        timeframe=timeframe,
                        # Получаем все исторические данные
                        since=None,
                        limit=1000
                    )
                    
                    if not ohlcv_data:
                        logger.warning(f"Не удалось получить данные для {symbol} на таймфрейме {timeframe}")
                        progress_bar.update(1)  # Обновляем прогресс-бар даже при ошибке
                        continue
                    
                    # Обрабатываем OHLCV данные и рассчитываем индикаторы
                    market_data_list = self.data_processor.process_ohlcv_with_indicators(ohlcv_data)
                    
                    # Добавляем к общему списку
                    all_market_data.extend(market_data_list)
                    
                    # Обновляем прогресс-бар
                    progress_bar.update(1)
                    
                    # Показываем текущий прогресс
                    total_records = len(all_market_data)
                    progress_percent = (progress_bar.n / progress_bar.total) * 100
                    estimated_total = int(total_records / (progress_percent / 100) if progress_percent > 0 else 0)
                    
                    print(f"Прогресс: {progress_percent:.1f}% | Собрано записей: {total_records} | "
                          f"Ожидаемый размер датасета: ~{estimated_total} записей")
                    
                    # Добавляем задержку между запросами
                    time.sleep(1)
                
                # Получаем текущий стакан для символа
                orderbook = self.exchange_service.fetch_orderbook(symbol)
                
                # Если получен стакан, добавляем его к последней записи для этого символа
                if orderbook:
                    # Ищем последние записи для каждого таймфрейма этого символа
                    for timeframe in self.timeframes:
                        for i in range(len(all_market_data) - 1, -1, -1):
                            if all_market_data[i].ohlcv.symbol == symbol and all_market_data[i].ohlcv.timeframe == timeframe:
                                # Обновляем стакан для последней записи каждого таймфрейма
                                all_market_data[i].order_book_snapshot = orderbook
                                break
            
            # Закрываем прогресс-бар
            progress_bar.close()
            
            # Сохраняем собранные данные
            if all_market_data:
                logger.info(f"Сохранение {len(all_market_data)} записей в CSV")
                print(f"Сохранение {len(all_market_data)} записей в CSV файл {config.DATASET_PATH}")
                success = self.data_storage.append_to_dataset(all_market_data)
                if success:
                    logger.info("Данные успешно сохранены")
                    print("✅ Данные успешно сохранены")
                else:
                    logger.error("Ошибка при сохранении данных")
                    print("❌ Ошибка при сохранении данных")
                    return False
            else:
                logger.warning("Нет данных для сохранения")
                print("⚠️ Нет данных для сохранения")
                return False
            
            logger.info(f"Сбор данных завершен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Сбор данных завершен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сборе данных: {e}")
            print(f"❌ Ошибка при сборе данных: {e}")
            return False
    
    def start(self):
        """Запускает микросервис с периодическим сбором данных"""
        logger.info("Запуск микросервиса сбора данных")
        print("🚀 Запуск микросервиса сбора данных")
        
        # Выводим оценку времени сбора данных
        total_requests = len(self.symbols) * len(self.timeframes)
        estimated_time_minutes = total_requests * 1.5  # ~1.5 минуты на пару/таймфрейм
        print(f"Оценочное время полного сбора данных: ~{estimated_time_minutes:.1f} минут")
        print(f"Ожидаемый размер датасета: ~{total_requests * 350} записей")
        
        # Выполняем первоначальный сбор данных
        self.collect_data()
        
        # Настраиваем периодический сбор данных
        interval_minutes = self.collection_interval // 60
        logger.info(f"Настройка периодического сбора данных каждые {interval_minutes} минут")
        print(f"⏰ Настройка периодического сбора данных каждые {interval_minutes} минут")
        
        # Планируем задачу с использованием библиотеки schedule
        schedule.every(interval_minutes).minutes.do(self.collect_data)
        
        # Запускаем бесконечный цикл для выполнения запланированных задач
        logger.info("Микросервис запущен и ожидает выполнения запланированных задач")
        print("🔄 Микросервис запущен и ожидает выполнения запланированных задач")
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    # Создаем и запускаем микросервис сбора данных
    collector = DataCollector()
    collector.start()