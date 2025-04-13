import pandas as pd
import numpy as np
import ta
from typing import Dict, Any, List

from src.logger import get_logger

# Получаем логгер
logger = get_logger("indicators")


def calculate_indicators(ohlcv_df: pd.DataFrame) -> pd.DataFrame:
    """
    Рассчитывает технические индикаторы на основе OHLCV данных
    
    Args:
        ohlcv_df: DataFrame с колонками ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        
    Returns:
        DataFrame с добавленными техническими индикаторами
    """
    if ohlcv_df.empty:
        logger.warning("Предоставлен пустой DataFrame, индикаторы не рассчитаны")
        return pd.DataFrame()
    
    # Создаем копию для работы
    df = ohlcv_df.copy()
    
    # Убедимся, что данные отсортированы по времени
    df = df.sort_values('timestamp')
    
    # Получаем информацию о временном ряде
    symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else 'Unknown'
    timeframe = df['timeframe'].iloc[0] if 'timeframe' in df.columns else 'Unknown'
    
    logger.debug(f"Расчет технических индикаторов для {symbol} на таймфрейме {timeframe}, {len(df)} записей")
    
    try:
        # SMA - Simple Moving Average (простая скользящая средняя)
        df['sma_20'] = ta.trend.sma_indicator(df['close'], window=20)
        df['sma_50'] = ta.trend.sma_indicator(df['close'], window=50)
        df['sma_200'] = ta.trend.sma_indicator(df['close'], window=200)
        
        # EMA - Exponential Moving Average (экспоненциальная скользящая средняя)
        df['ema_12'] = ta.trend.ema_indicator(df['close'], window=12)
        df['ema_26'] = ta.trend.ema_indicator(df['close'], window=26)
        
        # MACD - Moving Average Convergence Divergence
        macd = ta.trend.MACD(df['close'], window_slow=26, window_fast=12, window_sign=9)
        df['macd'] = macd.macd()
        df['macd_signal'] = macd.macd_signal()
        df['macd_hist'] = macd.macd_diff()
        
        # RSI - Relative Strength Index (индекс относительной силы)
        df['rsi_14'] = ta.momentum.rsi(df['close'], window=14)
        
        # Bollinger Bands (полосы Боллинджера)
        bollinger = ta.volatility.BollingerBands(df['close'], window=20, window_dev=2)
        df['bb_upper'] = bollinger.bollinger_hband()
        df['bb_middle'] = bollinger.bollinger_mavg()
        df['bb_lower'] = bollinger.bollinger_lband()
        
        # ATR - Average True Range (средний истинный диапазон)
        df['atr_14'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], window=14)
        
        # OBV - On Balance Volume (объем балансировки)
        df['obv'] = ta.volume.on_balance_volume(df['close'], df['volume'])
        
        logger.debug(f"Рассчитано {len([col for col in df.columns if col not in ohlcv_df.columns])} индикаторов")
        
    except Exception as e:
        logger.error(f"Ошибка при расчете индикаторов: {e}")
    
    return df


def convert_to_technical_indicators(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Преобразует строку с техническими индикаторами в формат для модели TechnicalIndicators
    
    Args:
        row: Строка DataFrame с рассчитанными индикаторами
        
    Returns:
        Словарь с данными для модели TechnicalIndicators
    """
    indicators = {
        "symbol": row["symbol"],
        "timeframe": row["timeframe"],
        "timestamp": row["timestamp"],
        "datetime": row["datetime"],
    }
    
    # Добавляем все индикаторы из строки
    indicator_fields = [
        "sma_20", "sma_50", "sma_200", "ema_12", "ema_26",
        "rsi_14", "macd", "macd_signal", "macd_hist",
        "bb_upper", "bb_middle", "bb_lower", "atr_14", "obv"
    ]
    
    for field in indicator_fields:
        if field in row:
            indicators[field] = row[field]
    
    return indicators 