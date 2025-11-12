create table dds.sales as  
WITH detailed_data AS (
    -- Детальные данные по году, неделе, городу и value
    SELECT 
        year,
        week,
        c.name_en as city,
        case
            when value is null then 'Без ответа'
            else value
        end as value_group,
        ROUND(SUM(amount)) AS total_amount,
        SUM(CASE WHEN item_category = 'Билеты' THEN r.qty ELSE 0 END) AS total_tickets,
        SUM(an.qty) as total_ads_qty
    FROM raw.receipts r
    JOIN dds.cities c ON r.kkm = c.kkm_name 
    JOIN public.dim_dates dd ON r.sale_date::date = dd.date::date
    LEFT JOIN raw.ads_new_hash_new AS an 
        ON c.name_ru::text = an.city::text 
        AND r.sale_date::timestamp BETWEEN 
            (an.date - INTERVAL '10 seconds') AND 
            (an.date + INTERVAL '15 seconds')
        AND r.item_category = 'Билеты'
    WHERE kkm IS NOT NULL 
    GROUP BY year, week, c.name_en, value
    
    UNION ALL
    
    -- Итого по value для каждого города и недели
    SELECT 
        year,
        week,
        c.name_en as city,
        '_Итого_' as value_group,
        ROUND(SUM(amount)) AS total_amount,
        SUM(CASE WHEN item_category = 'Билеты' THEN r.qty ELSE 0 END) AS total_tickets,
        SUM(an.qty) as total_ads_qty
    FROM raw.receipts r
    JOIN dds.cities c ON r.kkm = c.kkm_name 
    JOIN public.dim_dates dd ON r.sale_date::date = dd.date::date
    LEFT JOIN raw.ads_new_hash_new AS an 
        ON c.name_ru::text = an.city::text 
        AND r.sale_date::timestamp BETWEEN 
            (an.date - INTERVAL '10 seconds') AND 
            (an.date + INTERVAL '15 seconds')
        AND r.item_category = 'Билеты'
    WHERE kkm IS NOT NULL 
    GROUP BY year, week, c.name_en
    
    UNION ALL
    
    -- Итого по городам для каждого value и недели
    SELECT 
        year,
        week,
        'Итого' as city,
        case
            when value is null then 'Без ответа'
            else value
        end as value_group,
        ROUND(SUM(amount)) AS total_amount,
        SUM(CASE WHEN item_category = 'Билеты' THEN r.qty ELSE 0 END) AS total_tickets,
        SUM(an.qty) as total_ads_qty
    FROM raw.receipts r
    JOIN dds.cities c ON r.kkm = c.kkm_name 
    JOIN public.dim_dates dd ON r.sale_date::date = dd.date::date
    LEFT JOIN raw.ads_new_hash_new AS an 
        ON c.name_ru::text = an.city::text 
        AND r.sale_date::timestamp BETWEEN 
            (an.date - INTERVAL '10 seconds') AND 
            (an.date + INTERVAL '15 seconds')
        AND r.item_category = 'Билеты'
    WHERE kkm IS NOT NULL 
    GROUP BY year, week, value
    
    UNION ALL
    
    -- Общие итоги по неделям
    SELECT 
        year,
        week,
        'Итого' as city,
        '_Итого_' as value_group,
        ROUND(SUM(amount)) AS total_amount,
        SUM(CASE WHEN item_category = 'Билеты' THEN r.qty ELSE 0 END) AS total_tickets,
        SUM(an.qty) as total_ads_qty
    FROM raw.receipts r
    JOIN dds.cities c ON r.kkm = c.kkm_name 
    JOIN public.dim_dates dd ON r.sale_date::date = dd.date::date
    LEFT JOIN raw.ads_new_hash_new AS an 
        ON c.name_ru::text = an.city::text 
        AND r.sale_date::timestamp BETWEEN 
            (an.date - INTERVAL '10 seconds') AND 
            (an.date + INTERVAL '15 seconds')
        AND r.item_category = 'Билеты'
    WHERE kkm IS NOT NULL 
    GROUP BY year, week
)
SELECT 
    year,
    week,
    city,
    value_group,
    total_amount,
    total_tickets,
    total_ads_qty
FROM detailed_data
WHERE year IS NOT NULL AND week IS NOT NULL
ORDER BY year desc, week desc, 
    CASE WHEN city = 'Итого' THEN 1 ELSE 0 END, city, 
    CASE WHEN value_group = '_Итого_' THEN 1 ELSE 0 END, value_group;