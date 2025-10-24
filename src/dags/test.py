# Каждые 10 секунд
from src.croner import DAG

# cron (каждую минуту с 9 до 18 по будням)
frequent_dag = DAG("frequent_updates", schedule_interval="*/1 9-18 * * 1-5")

@frequent_dag.task
def update_dashboard():
    print("Обновление дашборда...")
    return "Дашборд обновлен"

@frequent_dag.task  
def send_notifications():
    print("Отправка уведомлений...")