# cron_parser.py


class CronParser:
    """Парсер cron-выражений с поддержкой секунд"""

    @staticmethod
    def parse(cron_string):
        """Парсит cron-строку и возвращает расписание"""
        parts = cron_string.strip().split()

        # Поддержка формата с секундами (6 частей) и без (5 частей)
        if len(parts) == 6:
            second, minute, hour, day, month, day_of_week = parts
        elif len(parts) == 5:
            second = "0"  # По умолчанию 0 секунд
            minute, hour, day, month, day_of_week = parts
        else:
            raise ValueError(
                f"Неверный формат cron: {cron_string}. Ожидается 5 или 6 частей"
            )

        result = {
            "second": CronParser._parse_part(second, 0, 59),
            "minute": CronParser._parse_part(minute, 0, 59),
            "hour": CronParser._parse_part(hour, 0, 23),
            "day": CronParser._parse_part(day, 1, 31),
            "month": CronParser._parse_part(month, 1, 12),
            "day_of_week": CronParser._parse_part(
                day_of_week, 0, 6
            ),  # 0=воскресенье, 6=суббота
        }

        return result

    @staticmethod
    def _parse_part(part, min_val, max_val):
        """Парсит одну часть cron-выражения"""
        if part == "*":
            result = list(range(min_val, max_val + 1))
        elif "-" in part:
            start, end = map(int, part.split("-"))
            result = list(range(start, end + 1))
        elif part.startswith("*/"):
            step = int(part[2:])
            result = list(range(min_val, max_val + 1, step))
        elif "," in part:
            result = [int(x) for x in part.split(",")]
        else:
            result = [int(part)]
        return result
