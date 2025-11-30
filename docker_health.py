import time
from src.config import config, logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError



def database_health_check(max_retries=30, retry_interval=5):
    """
    Health check для проверки доступности базы данных из контейнера
    
    Args:
        max_retries (int): Максимальное количество попыток подключения
        retry_interval (int): Интервал между попытками в секундах
    
    Returns:
        bool: True если база доступна, False в противном случае
    """
    engine = None
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Создаем подключение к базе данных
            engine = create_engine(config.db_config.get_url())
            
            # Пытаемся выполнить простой запрос для проверки соединения
            with engine.begin() as connection:
                result = connection.execute(text("SELECT 1"))
                test_value = result.scalar()
                
                if test_value == 1:
                    logger.info("✅ База данных доступна и отвечает на запросы")
                    return True
                else:
                    logger.warning("❌ База данных отвечает, но вернула неожиданный результат")
                    
        except SQLAlchemyError as e:
            retry_count += 1
            logger.warning(f"⚠️ Попытка {retry_count}/{max_retries}: База данных недоступна - {str(e)}")
            
            if retry_count < max_retries:
                logger.info(f"⏳ Ожидание {retry_interval} секунд перед следующей попыткой...")
                time.sleep(retry_interval)
            else:
                logger.error("❌ Превышено максимальное количество попыток подключения к базе данных")
                return False
                
        except Exception as e:
            logger.error(f"❌ Непредвиденная ошибка при проверке базы данных: {str(e)}")
            return False
            
        finally:
            # Закрываем соединение
            if engine:
                engine.dispose()
    
    return False

def call_load_offline_sales():
    """Задача для вызова SQL процедуры загрузки оффлайн продаж"""
    try:
        # Сначала проверяем доступность базы данных
        if not database_health_check():
            logger.error("❌ Невозможно выполнить процедуру: база данных недоступна")
            return
        
        engine = create_engine(config.db_config.get_url())

        with engine.begin() as connection:
            # Вызываем хранимую процедуру
            result = connection.execute(text("CALL dds.load_offline_sales();"))
            logger.info("✅ Успешно выполнена процедура dds.load_offline_sales()")

    except Exception as e:
        logger.error("❌ Ошибка при выполнении процедуры dds.load_offline_sales()", e)