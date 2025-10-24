import re
from datetime import datetime, timedelta

from .cron_parser import CronParser


class DAG:
    def __init__(self, dag_id, schedule_interval=None):
        self.dag_id = dag_id
        self.schedule_interval = schedule_interval
        self.tasks = []
        self.last_run = None
        self.next_run = None
        self.cron_schedule = None

        if schedule_interval and self._is_cron_string(schedule_interval):
            try:
                self.cron_schedule = CronParser.parse(schedule_interval)
                # Вычисляем первое следующее время при инициализации
                self.next_run = self._calculate_next_run(datetime.now())
                print(f"✅ DAG {dag_id} создан. Следующий запуск: {self.next_run}")
            except Exception as e:
                print(f"❌ Ошибка парсинга cron: {e}")

    def _is_cron_string(self, schedule_str):
        """Проверяет, является ли строка cron-выражением"""
        if not isinstance(schedule_str, str):
            return False
        parts = schedule_str.strip().split()
        return len(parts) in [5, 6] and all(self._is_cron_part(part) for part in parts)

    def _is_cron_part(self, part):
        """Проверяет валидность части cron-выражения"""
        cron_pattern = r"^(\*|\d+(-\d+)?(,\d+(-\d+)?)*|(\*\/\d+))$"
        return bool(re.match(cron_pattern, part))

    def task(self, func):
        """Декоратор для добавления задачи в DAG"""
        self.tasks.append(func)
        return func

    def _calculate_next_run(self, current_time):
        """Резервный алгоритм с оптимизированными итерациями"""
        next_time = current_time.replace(microsecond=0) + timedelta(seconds=1)

        # Группируем итерации по дням для больших интервалов
        days_checked = 0
        max_days = 366

        while days_checked < max_days:
            # Проверяем день месяца и месяц
            if (
                next_time.day in self.cron_schedule["day"]
                and next_time.month in self.cron_schedule["month"]
                and next_time.weekday() in self.cron_schedule["day_of_week"]
            ):
                # Внутри дня ищем подходящее время
                for hour in sorted(self.cron_schedule["hour"]):
                    if hour < next_time.hour:
                        continue
                    for minute in sorted(self.cron_schedule["minute"]):
                        if hour == next_time.hour and minute < next_time.minute:
                            continue
                        for second in sorted(self.cron_schedule.get("second", [0])):
                            candidate = next_time.replace(
                                hour=hour, minute=minute, second=second
                            )
                            if candidate > current_time:
                                return candidate

            # Переходим к следующему дню
            next_time = next_time.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=1)
            days_checked += 1

        raise ValueError(f"Не удалось найти следующее время запуска за {max_days} дней")

    def should_run(self, current_time):
        """Определяет, нужно ли запускать DAG на основе следующего времени"""
        # Для однократного запуска
        if self.schedule_interval == "once":
            if self.last_run is None:
                self.next_run = current_time
                return True
            return False

        # Для ежедневного запуска
        elif self.schedule_interval == "daily":
            if self.last_run is None:
                self.next_run = current_time.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                if self.next_run <= current_time:
                    self.next_run += timedelta(days=1)
                return True
            next_day = self.last_run.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=1)
            if current_time >= next_day:
                self.next_run = next_day
                return True
            return False

        # Для ежечасного запуска
        elif self.schedule_interval == "hourly":
            if self.last_run is None:
                self.next_run = current_time.replace(minute=0, second=0, microsecond=0)
                if self.next_run <= current_time:
                    self.next_run += timedelta(hours=1)
                return True
            next_hour = self.last_run.replace(
                minute=0, second=0, microsecond=0
            ) + timedelta(hours=1)
            if current_time >= next_hour:
                self.next_run = next_hour
                return True
            return False

        # Для cron-расписания
        elif self.cron_schedule:
            # Если следующее время еще не вычислено
            if self.next_run is None:
                self.next_run = self._calculate_next_run(current_time)
                return False

            # Если текущее время >= следующего запланированного, запускаем
            if current_time >= self.next_run:
                return True

        return False

    def run(self):
        """Запуск всех задач DAG с обновлением следующего времени"""
        current_time = datetime.now()
        print(f"🚀 Запуск DAG: {self.dag_id} в {current_time}")

        # Обновляем время последнего запуска
        self.last_run = current_time

        # Вычисляем следующее время запуска
        if self.cron_schedule:
            old_next_run = self.next_run
            self.next_run = self._calculate_next_run(current_time)
            print(
                f"📅 Следующий запуск DAG {self.dag_id}: {self.next_run} (было: {old_next_run})"
            )
        elif self.schedule_interval in ["daily", "hourly"]:
            print(f"📅 Следующий запуск DAG {self.dag_id}: {self.next_run}")

        # Выполняем задачи
        success_count = 0
        error_count = 0

        for task in self.tasks:
            try:
                start_time = datetime.now()
                result = task()
                execution_time = (datetime.now() - start_time).total_seconds()

                print(
                    f"✅ Задача {task.__name__} выполнена успешно за {execution_time:.2f}с"
                )
                if result is not None:
                    print(f"📊 Результат: {result}")
                success_count += 1

            except Exception as e:
                print(f"❌ Ошибка в задаче {task.__name__}: {e}")
                error_count += 1

        # Итоги выполнения
        status = "✅ УСПЕШНО" if error_count == 0 else "⚠️  С ОШИБКАМИ"
        print(
            f"🎯 DAG {self.dag_id} завершен {status}. "
            f"Задачи: {success_count}✅ {error_count}❌"
        )

    def get_status(self):
        """Возвращает статус DAG для мониторинга"""
        status = {
            "dag_id": self.dag_id,
            "schedule_interval": self.schedule_interval,
            "last_run": self.last_run.isoformat() if self.last_run else "Никогда",
            "next_run": self.next_run.isoformat()
            if self.next_run
            else "Не запланирован",
            "tasks_count": len(self.tasks),
            "cron_schedule": self.cron_schedule,
        }
        return status

    def __str__(self):
        """Строковое представление DAG"""
        last_run_str = (
            self.last_run.strftime("%Y-%m-%d %H:%M:%S") if self.last_run else "Никогда"
        )
        next_run_str = (
            self.next_run.strftime("%Y-%m-%d %H:%M:%S")
            if self.next_run
            else "Не запланирован"
        )

        return (
            f"DAG('{self.dag_id}', schedule='{self.schedule_interval}', "
            f"last_run={last_run_str}, next_run={next_run_str}, "
            f"tasks={len(self.tasks)})"
        )
