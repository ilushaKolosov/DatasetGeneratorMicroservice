import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import datetime

from src.models.data_models import OHLCV, TechnicalIndicators, MarketData, OrderBook
from src.utils.indicators import calculate_indicators, convert_to_technical_indicators
from src.logger import get_logger

# Получаем логгер
logger = get_logger("data_processor")


class DataProcessor:
    """Сервис для обработки рыночных данных и расчета технических индикаторов"""
    
    def __init__(self):
        """Инициализирует сервис обработки данных"""
        pass
    
    def ohlcv_to_dataframe(self, ohlcv_list: List[OHLCV]) -> pd.DataFrame:
        """
        Преобразует список объектов OHLCV в DataFrame для дальнейшей обработки
        
        Args:
            ohlcv_list: Список объектов OHLCV
            
        Returns:
            DataFrame с OHLCV данными
        """
        if not ohlcv_list:
            return pd.DataFrame()
        
        # Преобразуем каждый объект OHLCV в словарь
        data = []
        for ohlcv in ohlcv_list:
            data.append(ohlcv.dict())
        
        # Создаем DataFrame из списка словарей
        df = pd.DataFrame(data)
        
        # Убедимся, что данные отсортированы по времени
        df = df.sort_values('timestamp')
        
        return df
    
    def calculate_technical_indicators(self, ohlcv_list: List[OHLCV]) -> List[TechnicalIndicators]:
        """
        Рассчитывает технические индикаторы для списка OHLCV данных
        
        Args:
            ohlcv_list: Список объектов OHLCV
            
        Returns:
            Список объектов TechnicalIndicators
        """
        if not ohlcv_list:
            return []
        
        # Преобразуем список OHLCV в DataFrame
        df = self.ohlcv_to_dataframe(ohlcv_list)
        
        # Рассчитываем технические индикаторы
        df_with_indicators = calculate_indicators(df)
        
        # Преобразуем DataFrame с индикаторами в список объектов TechnicalIndicators
        indicators_list = []
        for _, row in df_with_indicators.iterrows():
            # Преобразуем строку в словарь для модели
            indicator_data = convert_to_technical_indicators(row.to_dict())
            
            # Создаем объект TechnicalIndicators
            indicator = TechnicalIndicators(**indicator_data)
            indicators_list.append(indicator)
        
        return indicators_list
    
    def merge_data(self, ohlcv: OHLCV, indicators: Optional[TechnicalIndicators] = None, 
                   orderbook: Optional[OrderBook] = None) -> MarketData:
        """
        Объединяет OHLCV данные, технические индикаторы и данные стакана в единый объект
        
        Args:
            ohlcv: Объект OHLCV
            indicators: Объект TechnicalIndicators
            orderbook: Объект OrderBook
            
        Returns:
            Объект MarketData, объединяющий все данные
        """
        # Если индикаторы не предоставлены, создаем пустой объект с базовыми полями
        if indicators is None:
            indicators = TechnicalIndicators(
                symbol=ohlcv.symbol,
                timeframe=ohlcv.timeframe,
                timestamp=ohlcv.timestamp,
                datetime=ohlcv.datetime
            )
        
        # Создаем и возвращаем объединенный объект MarketData
        return MarketData(
            ohlcv=ohlcv,
            indicators=indicators,
            order_book_snapshot=orderbook
        )
    
    def process_ohlcv_with_indicators(self, ohlcv_list: List[OHLCV]) -> List[MarketData]:
        """
        Обрабатывает список OHLCV данных, рассчитывает индикаторы и объединяет их
        
        Args:
            ohlcv_list: Список объектов OHLCV
            
        Returns:
            Список объектов MarketData с расчитанными индикаторами
        """
        if not ohlcv_list:
            return []
        
        # Рассчитываем технические индикаторы
        indicators_list = self.calculate_technical_indicators(ohlcv_list)
        
        # Создаем словарь для быстрого поиска индикаторов по временной метке
        indicators_dict = {indicator.timestamp: indicator for indicator in indicators_list}
        
        # Объединяем OHLCV с индикаторами
        result = []
        for ohlcv in ohlcv_list:
            # Находим индикаторы для данной свечи
            indicator = indicators_dict.get(ohlcv.timestamp)
            
            # Объединяем данные
            market_data = self.merge_data(ohlcv, indicator)
            result.append(market_data)
        
        return result
    
    def dataframe_to_csv(self, market_data_list: List[MarketData]) -> pd.DataFrame:
        """
        Преобразует список объектов MarketData в плоский DataFrame для CSV
        
        Args:
            market_data_list: Список объектов MarketData
            
        Returns:
            DataFrame с плоской структурой для сохранения в CSV
        """
        if not market_data_list:
            return pd.DataFrame()
        
        # Преобразуем каждый объект MarketData в плоский словарь
        data = [item.to_dict() for item in market_data_list]
        
        # Создаем DataFrame из списка словарей
        df = pd.DataFrame(data)
        
        # Сортируем по символу, временному интервалу и времени
        df = df.sort_values(['symbol', 'timeframe', 'timestamp'])
        
        return df 