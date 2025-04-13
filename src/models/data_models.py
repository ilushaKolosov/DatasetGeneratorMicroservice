from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from datetime import datetime


class OHLCV(BaseModel):
    """Модель для хранения данных OHLCV (Open, High, Low, Close, Volume)"""
    timestamp: int
    datetime: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    symbol: str
    timeframe: str


class OrderBookLevel(BaseModel):
    """Модель для уровня стакана (price и amount)"""
    price: float
    amount: float


class OrderBook(BaseModel):
    """Модель для хранения данных книги ордеров (стакана)"""
    timestamp: int
    datetime: datetime
    symbol: str
    bids: List[OrderBookLevel]  # Заявки на покупку
    asks: List[OrderBookLevel]  # Заявки на продажу
    
    
class TechnicalIndicators(BaseModel):
    """Модель для хранения рассчитанных технических индикаторов"""
    symbol: str
    timeframe: str
    timestamp: int
    datetime: datetime
    
    # Трендовые индикаторы
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    
    # Осцилляторы
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None
    
    # Волатильность
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    atr_14: Optional[float] = None
    
    # Объемные индикаторы
    obv: Optional[float] = None
    
    
class MarketData(BaseModel):
    """Объединенная модель всех рыночных данных и индикаторов"""
    ohlcv: OHLCV
    indicators: TechnicalIndicators
    order_book_snapshot: Optional[OrderBook] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в плоский словарь для CSV"""
        data = {
            "timestamp": self.ohlcv.timestamp,
            "datetime": self.ohlcv.datetime,
            "symbol": self.ohlcv.symbol,
            "timeframe": self.ohlcv.timeframe,
            "open": self.ohlcv.open,
            "high": self.ohlcv.high,
            "low": self.ohlcv.low,
            "close": self.ohlcv.close,
            "volume": self.ohlcv.volume,
        }
        
        # Добавляем индикаторы
        for field, value in self.indicators.dict().items():
            if field not in ["symbol", "timeframe", "timestamp", "datetime"]:
                data[field] = value
        
        # Добавляем данные стакана, если они есть
        if self.order_book_snapshot:
            data["best_bid_price"] = self.order_book_snapshot.bids[0].price if self.order_book_snapshot.bids else None
            data["best_bid_amount"] = self.order_book_snapshot.bids[0].amount if self.order_book_snapshot.bids else None
            data["best_ask_price"] = self.order_book_snapshot.asks[0].price if self.order_book_snapshot.asks else None
            data["best_ask_amount"] = self.order_book_snapshot.asks[0].amount if self.order_book_snapshot.asks else None
            
            # Рассчитываем spread (разницу между лучшей ценой продажи и покупки)
            if self.order_book_snapshot.asks and self.order_book_snapshot.bids:
                data["spread"] = self.order_book_snapshot.asks[0].price - self.order_book_snapshot.bids[0].price
                data["spread_percent"] = (data["spread"] / self.order_book_snapshot.bids[0].price) * 100
        
        return data 