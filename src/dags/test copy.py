# Каждые 10 секунд
from src.croner import DAG
from src.config import logger

# cron (каждую минуту с 9 до 18 по будням)
frequent_dag = DAG("frequent_updates", schedule_interval="*/1 * * * *")


@frequent_dag.task
def update_dashboard():
    logger.info("Дашборд обновлен")
    return "Дашборд обновлен"


@frequent_dag.task
def send_notifications():
    print("Отправка уведомлений...")
