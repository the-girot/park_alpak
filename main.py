import time

from src.croner import Croner

# Пример использования с мониторингом
if __name__ == "__main__":
    croner = Croner("./src/dags")

    try:
        croner.start_scheduler(scan_interval=10)

        counter = 0
        while True:
            time.sleep(1)
            counter += 1

            # Каждые 30 секунд показываем статистику
            if counter % 120 == 0:
                print(croner.get_memory_stats())
    except KeyboardInterrupt:
        croner.stop_scheduler()
