import logging
from typing import Optional
import src.config as config

# Глобальная настройка логирования
logging.basicConfig(
    level=config.get_log_level(),
    format=config.LOG_FORMAT
)

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Возвращает настроенный логгер с указанным именем
    
    Args:
        name: Имя логгера
        
    Returns:
        Настроенный объект Logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(config.get_log_level())
    return logger 