import gc
import importlib.util
import threading
import time
import tracemalloc
import weakref
from collections import deque
from datetime import datetime
from pathlib import Path

import psutil

from src.config import logger

from .dag import DAG


class Croner:
    def __init__(self, dags_folder="./dags"):
        self.dags_folder = Path(dags_folder)
        self.dags = {}  # Может накапливаться
        self.running = False
        self.active_threads = weakref.WeakSet()  # Следим за активными потоками
        self.dag_queue = deque()  # Очередь DAG ожидающих выполнения
        self.queue_lock = threading.Lock()  # Блокировка для работы с очередью
        self.last_cleanup = datetime.now()
        self.memory_usage_log = []

        # Настройки параллелизма
        self.max_concurrent_dags = 5
        self.available_slots = threading.Semaphore(
            self.max_concurrent_dags
        )  # Семафор для ограничения параллелизма

        # Для мониторинга памяти
        tracemalloc.start()

    def load_dag_from_file(self, file_path):
        """Загружает DAG из Python файла с контролем памяти"""
        try:
            # Проверяем, не изменился ли файл
            dag_key = str(file_path)
            current_mtime = file_path.stat().st_mtime

            # Ищем существующий DAG по file_path (а не по dag_key)
            existing_dag_id = None
            for dag_id, dag_info in self.dags.items():
                if dag_info["file_path"] == dag_key:
                    existing_dag_id = dag_id
                    break

            # Если файл изменился или DAG еще не загружен, загружаем/перезагружаем
            if existing_dag_id is None:
                logger.info(f"🆕 Найден новый DAG файл: {file_path}")
            elif current_mtime > self.dags[existing_dag_id].get("mtime", 0):
                logger.warning(f"🔄 Файл {file_path} изменился, перезагружаем DAG")
                # Удаляем старый DAG
                del self.dags[existing_dag_id]
            else:
                # Файл не изменился, ничего не делаем
                return []

            spec = importlib.util.spec_from_file_location(
                f"dag_module_{file_path.stem}", file_path
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            loaded_dags = []
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, DAG):
                    dag_id = f"{file_path.stem}_{attr_name}"
                    self.dags[dag_id] = {
                        "dag": attr,
                        "mtime": current_mtime,
                        "file_path": dag_key,
                        "loaded_at": datetime.now(),
                    }
                    loaded_dags.append(dag_id)
                    logger.info(
                        f"✅ Загружен DAG: {dag_id} с расписанием: {attr.schedule_interval}"
                    )

            # Очищаем ссылки на модуль чтобы избежать циклических ссылок
            del module
            del spec

            return loaded_dags

        except Exception as e:
            print(f"❌ Ошибка загрузки DAG из {file_path}: {e}")
            return []

    def unload_dag(self, dag_id):
        """Выгружает DAG из памяти"""
        if dag_id in self.dags:
            print(f"Выгружаем DAG: {dag_id}")
            del self.dags[dag_id]
            return True
        return False

    def cleanup_old_dags(self):
        """Очищает DAG, файлы которых были удалены"""
        current_files = {
            str(p) for p in self.dags_folder.glob("*.py") if not p.name.startswith("_")
        }

        dags_to_remove = []
        for dag_id, dag_info in self.dags.items():
            if dag_info["file_path"] not in current_files:
                dags_to_remove.append(dag_id)

        for dag_id in dags_to_remove:
            self.unload_dag(dag_id)

        if dags_to_remove:
            logger.warning(f"Удалены DAG: {dags_to_remove}")

    def scan_dags_folder(self):
        """Сканирует папку с DAG и загружает новые с контролем памяти"""
        if not self.dags_folder.exists():
            self.dags_folder.mkdir(parents=True)
            logger.info(f"Создана папка для DAG: {self.dags_folder}")
            return

        # Очищаем удаленные DAG
        self.cleanup_old_dags()

        for file_path in self.dags_folder.glob("*.py"):
            if file_path.name.startswith("_"):
                continue

            dag_key = str(file_path)
            if dag_key not in [info["file_path"] for info in self.dags.values()]:
                logger.info(f"Найден новый DAG файл: {file_path}")
                self.load_dag_from_file(file_path)

    def run_dag_in_thread(self, dag_id, dag: DAG):
        """Запускает DAG в отдельном потоке с контролем ресурсов"""
        try:
            logger.info(f"Запуск DAG: {dag_id}")
            dag.run()
        except Exception as e:
            logger.critical(f"Ошибка при выполнении DAG {dag_id}: {e}")
        finally:
            # Освобождаем слот и убираем поток из отслеживания
            self.available_slots.release()
            if threading.current_thread() in self.active_threads:
                self.active_threads.remove(threading.current_thread())

            # Проверяем очередь после завершения DAG
            self.process_queue()

    def add_dag_to_queue(self, dag_id, dag: DAG):
        """Добавляет DAG в очередь на выполнение"""
        with self.queue_lock:
            # Проверяем, нет ли уже этого DAG в очереди
            for queued_dag_id, _ in self.dag_queue:
                if queued_dag_id == dag_id:
                    logger.info(f"DAG {dag_id} уже в очереди, пропускаем дублирование")
                    return

            self.dag_queue.append((dag_id, dag))
            logger.info(
                f"DAG {dag_id} добавлен в очередь. Размер очереди: {len(self.dag_queue)}"
            )

    def process_queue(self):
        """Обрабатывает очередь DAG, запуская их при наличии свободных слотов"""
        with self.queue_lock:
            while self.dag_queue and self.available_slots.acquire(blocking=False):
                dag_id, dag = self.dag_queue.popleft()
                logger.info(
                    f"Запуск DAG {dag_id} из очереди. Осталось в очереди: {len(self.dag_queue)}"
                )

                thread = threading.Thread(
                    target=self.run_dag_in_thread, args=(dag_id, dag), daemon=True
                )
                self.active_threads.add(thread)
                thread.start()

    def run_scheduled_dags(self):
        """Запускает DAG по расписанию с использованием очереди"""
        current_time = datetime.now()

        for dag_id, dag_info in self.dags.items():
            dag: DAG = dag_info["dag"]

            if dag.should_run(current_time):
                # Пытаемся запустить сразу если есть свободные слоты
                if self.available_slots.acquire(blocking=False):
                    logger.info(f"Немедленный запуск DAG: {dag_id}")
                    thread = threading.Thread(
                        target=self.run_dag_in_thread, args=(dag_id, dag), daemon=True
                    )
                    self.active_threads.add(thread)
                    thread.start()
                else:
                    # Если нет свободных слотов, добавляем в очередь
                    logger.info(
                        f"Свободных слотов нет, добавляем DAG {dag_id} в очередь"
                    )
                    self.add_dag_to_queue(dag_id, dag)

    def monitor_memory_usage(self):
        """Мониторинг использования памяти"""
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024

        # Логируем использование памяти
        self.memory_usage_log.append(
            {
                "timestamp": datetime.now(),
                "memory_mb": memory_mb,
                "active_dags": len(self.dags),
                "active_threads": sum(1 for t in self.active_threads if t.is_alive()),
                "queue_size": len(self.dag_queue),
                "available_slots": self.available_slots._value,  # Текущее количество доступных слотов
            }
        )

        # Держим только последние 100 записей
        if len(self.memory_usage_log) > 100:
            self.memory_usage_log.pop(0)

        # Предупреждение при высоком использовании памяти
        if memory_mb > 500:  # 500 MB
            logger.warning(
                f"⚠️  ВНИМАНИЕ: Высокое использование памяти: {memory_mb:.2f} MB"
            )

        return memory_mb

    def force_garbage_collection(self):
        """Принудительная сборка мусора"""
        collected = gc.collect()
        print(f"Сборка мусора: освобождено {collected} объектов")

    def periodic_cleanup(self):
        """Периодическая очистка ресурсов"""
        current_time = datetime.now()

        # Выполняем очистку каждые 10 минут
        if (current_time - self.last_cleanup).total_seconds() > 600:
            print("Выполняем периодическую очистку...")

            # Принудительная сборка мусора
            self.force_garbage_collection()

            # Мониторинг памяти
            memory_usage = self.monitor_memory_usage()
            print(f"Использование памяти: {memory_usage:.2f} MB")
            print(f"Активных DAG: {len(self.dags)}")
            print(
                f"Активных потоков: {sum(1 for t in self.active_threads if t.is_alive())}"
            )
            print(f"DAG в очереди: {len(self.dag_queue)}")

            self.last_cleanup = current_time

    def start_scheduler(self, scan_interval=30):
        """Запускает планировщик с контролем памяти"""

        def scheduler_loop():
            while self.running:
                try:
                    self.scan_dags_folder()
                    self.run_scheduled_dags()
                    self.periodic_cleanup()
                except Exception as e:
                    print(f"Ошибка в планировщике: {e}")
                time.sleep(scan_interval)

        self.running = True
        scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
        scheduler_thread.start()
        print(f"Планировщик запущен. Сканирование каждые {scan_interval} секунд")
        print(f"Максимум параллельных DAG: {self.max_concurrent_dags}")

    def stop_scheduler(self):
        """Останавливает планировщик и очищает ресурсы"""
        self.running = False
        print("Останавливаем планировщик...")

        # Очищаем очередь
        with self.queue_lock:
            self.dag_queue.clear()

        # Ждем завершения активных потоков (максимум 30 секунд)
        timeout = 30
        start_time = time.time()

        active_threads = [t for t in self.active_threads if t.is_alive()]
        if active_threads:
            print(f"Ожидаем завершения {len(active_threads)} активных DAG...")
            for thread in active_threads:
                thread.join(timeout=timeout - (time.time() - start_time))

        # Очищаем все DAG
        self.dags.clear()
        self.active_threads.clear()

        # Финальная сборка мусора
        self.force_garbage_collection()

        print("Планировщик остановлен")

    def get_memory_stats(self):
        """Возвращает статистику использования памяти"""
        if not self.memory_usage_log:
            return "Нет данных о памяти"

        current = self.memory_usage_log[-1]
        return (
            f"Память: {current['memory_mb']:.2f} MB, "
            f"DAG: {current['active_dags']}, "
            f"Потоки: {current['active_threads']}, "
            f"Очередь: {current['queue_size']}"
        )

    def get_queue_status(self):
        """Возвращает статус очереди"""
        with self.queue_lock:
            queue_dags = [dag_id for dag_id, _ in self.dag_queue]
            return {
                "queue_size": len(self.dag_queue),
                "queued_dags": queue_dags,
                "available_slots": self.available_slots._value,
                "max_concurrent": self.max_concurrent_dags,
            }
