import ccxt
import pandas as pd
from datetime import datetime, timedelta
import time
from typing import List, Dict, Any, Optional

from src.models.data_models import OHLCV, OrderBook, OrderBookLevel
import src.config as config
from src.logger import get_logger

# Получаем логгер
logger = get_logger("exchange_service")


class ExchangeService:
    """Сервис для взаимодействия с биржей и получения исторических/текущих данных"""
    
    def __init__(self, exchange_id: str = config.EXCHANGE, api_key: str = config.BINANCE_API_KEY, 
                 api_secret: str = config.BINANCE_API_SECRET):
        """
        Инициализирует сервис для работы с биржей
        
        Args:
            exchange_id: ID биржи (например, 'binance', 'kraken', 'coinbase')
            api_key: API ключ для доступа к бирже
            api_secret: Секретный ключ API
        """
        self.exchange_id = exchange_id
        self.api_key = api_key
        self.api_secret = api_secret
        
        # Инициализация клиента биржи
        self._init_exchange()
        
    def _init_exchange(self):
        """Инициализирует клиент биржи"""
        try:
            # Создаем экземпляр биржи с API ключами
            exchange_class = getattr(ccxt, self.exchange_id)
            self.exchange = exchange_class({
                'apiKey': self.api_key,
                'secret': self.api_secret,
                'enableRateLimit': config.RATE_LIMIT_ENABLED,
                'timeout': config.REQUEST_TIMEOUT,
            })
            
            # Добавляем настройку для автоматической синхронизации времени
            self.exchange.options['adjustForTimeDifference'] = True
            
            # Загружаем рынки для получения информации о торговых парах
            self.exchange.load_markets()
            logger.info(f"Успешно инициализирован клиент биржи {self.exchange_id}")
            
        except Exception as e:
            logger.error(f"Ошибка при инициализации биржи: {e}")
            raise
    
    def fetch_historical_ohlcv(self, symbol: str, timeframe: str, 
                               since: Optional[int] = None, limit: int = 1000) -> List[OHLCV]:
        """
        Получает исторические OHLCV данные с биржи
        
        Args:
            symbol: Символ торговой пары (например, 'BTC/USDT')
            timeframe: Временной интервал (например, '1h', '4h', '1d')
            since: Unix timestamp в миллисекундах для начала периода
            limit: Максимальное количество свечей для получения
            
        Returns:
            Список объектов OHLCV
        """
        try:
            # Если начальное время не указано, используем значение по умолчанию
            if since is None:
                # Получаем данные за последние N дней
                since = int((datetime.now() - timedelta(days=config.HISTORY_DAYS)).timestamp() * 1000)
            
            logger.info(f"Получение исторических данных для {symbol} на таймфрейме {timeframe} с {datetime.fromtimestamp(since/1000)}")
            
            # Получаем данные с биржи
            ohlcv_data = self.exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            
            # Преобразуем в список объектов OHLCV
            result = []
            for item in ohlcv_data:
                timestamp, open_price, high, low, close, volume = item
                
                ohlcv = OHLCV(
                    timestamp=timestamp,
                    datetime=datetime.fromtimestamp(timestamp / 1000),
                    open=open_price,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                    symbol=symbol,
                    timeframe=timeframe
                )
                result.append(ohlcv)
            
            logger.info(f"Получено {len(result)} OHLCV записей для {symbol} на таймфрейме {timeframe}")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при получении исторических данных: {e}")
            return []
    
    def fetch_orderbook(self, symbol: str, limit: int = 20) -> Optional[OrderBook]:
        """
        Получает текущий стакан (книгу ордеров) для указанной торговой пары
        
        Args:
            symbol: Символ торговой пары (например, 'BTC/USDT')
            limit: Глубина стакана (количество уровней цен)
            
        Returns:
            Объект OrderBook или None в случае ошибки
        """
        try:
            # Получаем стакан с биржи
            orderbook_data = self.exchange.fetch_order_book(symbol, limit)
            
            # Извлекаем временную метку или используем текущее время, если не предоставлено
            timestamp = orderbook_data.get('timestamp', int(time.time() * 1000))
            
            # Преобразуем биды и аски в объекты OrderBookLevel
            bids = [OrderBookLevel(price=price, amount=amount) for price, amount in orderbook_data['bids']]
            asks = [OrderBookLevel(price=price, amount=amount) for price, amount in orderbook_data['asks']]
            
            # Создаем объект OrderBook
            orderbook = OrderBook(
                timestamp=timestamp,
                datetime=datetime.fromtimestamp(timestamp / 1000),
                symbol=symbol,
                bids=bids,
                asks=asks
            )
            
            return orderbook
            
        except Exception as e:
            logger.error(f"Ошибка при получении стакана для {symbol}: {e}")
            return None
    
    def fetch_all_data_for_symbols(self, symbols: List[str], timeframes: List[str]) -> Dict[str, Dict[str, List[OHLCV]]]:
        """
        Получает исторические данные для списка символов и временных интервалов
        
        Args:
            symbols: Список символов торговых пар
            timeframes: Список временных интервалов
            
        Returns:
            Вложенный словарь {symbol: {timeframe: [OHLCV]}}
        """
        result = {}
        
        for symbol in symbols:
            result[symbol] = {}
            for timeframe in timeframes:
                logger.info(f"Получение данных для {symbol} на таймфрейме {timeframe}")
                result[symbol][timeframe] = self.fetch_historical_ohlcv(symbol, timeframe)
                
                # Добавляем задержку, чтобы не превысить лимиты запросов к API
                time.sleep(1)
        
        return result 