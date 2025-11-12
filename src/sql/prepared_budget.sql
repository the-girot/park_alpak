drop table dds.prepared_budgets
CREATE TABLE dds.prepared_budgets AS
WITH cte AS (
    SELECT 
        dd.year,
        dd.week,
        "dateFrom"::date as date_from, 
        CASE 
            WHEN channel = 'Яндекс.Карты' THEN 'Яндекс.Карты'
            WHEN channel = 'Тик-Ток Посевы' THEN 'Тикток (соц.сеть)'
            WHEN channel = 'Инстаграм Посевы' THEN 'Инстаграм (соц.сеть)'
            WHEN channel = 'Гугл Карты' THEN 'Гугл.Карты'
            WHEN channel = '2gis' THEN 'Карты 2gis'
            WHEN channel = 'Телеграм Посевы' THEN 'Телеграм (мессенджер)'
            WHEN channel = 'ВК ADS' THEN 'Вконтакте (соц.сеть)'
            WHEN channel = 'Телеграм ADS' THEN 'Телеграм (мессенджер)' 
        END as channel,
        city, 
        CASE 
            WHEN channel = 'Инстаграм Посевы' THEN budget * 2 /3
            else budget
        END as price_numeric
    FROM raw.budgets as brn 
    JOIN public.dim_dates dd ON "dateFrom"::date = dd.date::date
    WHERE channel IS NOT NULL
)

-- Детальные данные
SELECT 
    year,
    week,
    date_from,
    city,
    channel,
    SUM(price_numeric) as current_week,
    'Детальные данные' as aggregation_level
FROM cte
WHERE city IS NOT NULL AND channel IS NOT NULL
GROUP BY year, week, date_from, city, channel

UNION ALL

-- Итого по каналам для каждого города
SELECT 
    year,
    week,
    date_from,
    city,
    'Итого' as channel,
    SUM(price_numeric) as current_week,
    'Итого по городу' as aggregation_level
FROM cte
WHERE city IS NOT NULL AND channel IS NOT NULL
GROUP BY year, week, date_from, city

UNION ALL

-- Итого по городам для каждого канала
SELECT 
    year,
    week,
    date_from,
    'Итого' as city,
    channel,
    SUM(price_numeric) as current_week,
    'Итого по каналу' as aggregation_level
FROM cte
WHERE city IS NOT NULL AND channel IS NOT NULL
GROUP BY year, week, date_from, channel

UNION ALL

-- Общие итоги
SELECT 
    year,
    week,
    date_from,
    'Итого' as city,
    'Итого' as channel,
    SUM(price_numeric) as current_week,
    'Общий итог' as aggregation_level
FROM cte
WHERE city IS NOT NULL AND channel IS NOT NULL
GROUP BY year, week, date_from

ORDER BY year, week, date_from, city, channel;