insert into dds.sales  
WITH 
prepared_data as (
    select 
        extract(year from r.дата) as year,
        extract(week from r.дата) as week,
        r.дата,
        c.name_en as city,
        value,
        case
            when value is null then 'Без ответа'
            else value
        end as value_group,
        r."Категория номенклатуры" as item_category,
        r."Количество" as qty_tickets,
        an.qty as qty_answers,
        -- Добавляем идентификатор чека для группировки
        CONCAT(r.дата::timestamp(0), '_', r.city) as check_id
    from public.offline_sales r
    JOIN dds.cities c ON r.city = c.name_en  
    LEFT JOIN raw.ads AS an 
        ON c.name_ru::text = an.city::text 
        AND an.date BETWEEN 
            (r.дата) AND 
            (r.дата + INTERVAL '8 seconds')
        AND r."Категория номенклатуры" = 'Билеты'
    where r."Категория номенклатуры" = 'Билеты' 
),
-- Группируем по чекам, чтобы один чек был одной записью
unique_checks as (
    select 
        year,
        week,
        city,
        value_group,
        SUM(qty_tickets) as qty_tickets,
        -- Берем максимальное значение qty_answers для чека (или можно использовать MIN/AVG)
        MAX(qty_answers) as qty_answers,
        check_id
    from prepared_data
    group by 
        year,
        week,
        city,
        value_group,
        check_id
)
select
    year,
    week,
    'Итого' as city,
    'Итого' as value_group,
    sum(qty_tickets)  as total_tickets,
    sum(qty_answers) as total_ads_qty
from unique_checks
group by 
    year,
    week

union all

select
    year,
    week,
    city,
    value_group,
    sum(qty_tickets)  as total_tickets,
    sum(qty_answers) as total_ads_qty
from unique_checks
group by 
    year,
    week,
    city,
    value_group
    
union all

select
    year,
    week,
    city,
    'Итого' as value_group,
    sum(qty_tickets),
    sum(qty_answers)
from unique_checks
group by 
    year,
    week,
    city
    
union all

select
    year,
    week,
    'Итого' as city,
    value_group,
    sum(qty_tickets) as total_tickets,
    sum(qty_answers) as total_ads_qty
from unique_checks
group by 
    year,
    week,
    value_group
order by year desc, week desc, city desc, value_group desc














create table dds.sales_month as 
WITH 
prepared_data as (
    select 
        extract(year from r.дата) as year,
        extract(month from r.дата) as month,
        r.дата,
        c.name_en as city,
        value,
        case
            when value is null then 'Без ответа'
            else value
        end as value_group,
        r."Категория номенклатуры" as item_category,
        r."Количество" as qty_tickets,
        an.qty as qty_answers,
        -- Добавляем идентификатор чека для группировки
        CONCAT(r.дата::timestamp(0), '_', r.city) as check_id
    from public.offline_sales r
    JOIN dds.cities c ON r.city = c.name_en  
    LEFT JOIN raw.ads AS an 
        ON c.name_ru::text = an.city::text 
        AND an.date BETWEEN 
            (r.дата) AND 
            (r.дата + INTERVAL '8 seconds')
        AND r."Категория номенклатуры" = 'Билеты'
    where r."Категория номенклатуры" = 'Билеты' 
),
-- Группируем по чекам, чтобы один чек был одной записью
unique_checks as (
    select 
        year,
        month,
        city,
        value_group,
        SUM(qty_tickets) as qty_tickets,
        -- Берем максимальное значение qty_answers для чека (или можно использовать MIN/AVG)
        MAX(qty_answers) as qty_answers,
        check_id
    from prepared_data
    group by 
        year,
        month,
        city,
        value_group,
        check_id
)
select
    year,
    month,
    'Итого' as city,
    'Итого' as value_group,
    sum(qty_tickets)  as total_tickets,
    sum(qty_answers) as total_ads_qty
from unique_checks
group by 
    year,
    month

union all

select
    year,
    month,
    city,
    value_group,
    sum(qty_tickets)  as total_tickets,
    sum(qty_answers) as total_ads_qty
from unique_checks
group by 
    year,
    month,
    city,
    value_group
    
union all

select
    year,
    month,
    city,
    'Итого' as value_group,
    sum(qty_tickets),
    sum(qty_answers)
from unique_checks
group by 
    year,
    month,
    city
    
union all

select
    year,
    month,
    'Итого' as city,
    value_group,
    sum(qty_tickets) as total_tickets,
    sum(qty_answers) as total_ads_qty
from unique_checks
group by 
    year,
    month,
    value_group
order by year desc, month desc, city desc, value_group desc