with prepared_data as (
select
    created_at::date,
    price_nominal - timepad_commission - acquiring_commission as amount,
    case 
            when ticket_type_name like '%орм%' then 0 
            else 1 
        end as is_ticket,
        0 as offline_ticket,
    c.name_en
from public.online_sales_simple oss
join dds.cities c on oss.event_id = c.event_id
where status_name in ('paid','transfer_payment')


union all

select
    created_at::date,
    sum(price_nominal - timepad_commission - acquiring_commission),
    sum(case 
            when ticket_type_name like '%орм%' then 0 
            else 1 
        end) as is_ticket,
        0 as offline_ticket,
    'Итого'
from public.online_sales_simple oss
join dds.cities c on oss.event_id = c.event_id
where status_name in ('paid','transfer_payment')
group by created_at::date, ticket_type_name

union all

select
    дата::date,
    "Выручка, ₽",
    CASE WHEN "Категория номенклатуры" = 'Билеты' THEN "Количество" ELSE 0 END AS total_tickets,
    CASE WHEN "Категория номенклатуры" = 'Билеты' THEN "Количество" ELSE 0 END AS offline_ticket,
    city
from public.offline_sales

union all 
select
    дата::date,
    sum("Выручка, ₽"),
    SUM(CASE WHEN "Категория номенклатуры" = 'Билеты' THEN "Количество" ELSE 0 END) AS total_tickets,
    SUM(CASE WHEN "Категория номенклатуры" = 'Билеты' THEN "Количество" ELSE 0 END) AS offline_ticket,
    'Итого'
from public.offline_sales
group by 
дата::date
),
all_amount_and_tickets as (
select 
    extract(year from created_at) as year,
    extract(week from created_at) as week,
    name_en as city,
    sum(amount) as total_amount,
    sum(is_ticket) as total_tickets,
    sum(offline_ticket) as offline_tickets,
    case 
        when sum(offline_ticket) = 0 then 0 
        else sum(amount) / sum(offline_ticket)
    end as sr_amount
from prepared_data
group by 
    extract(year from created_at),
    extract(week from created_at),
    name_en
) ,
        
-- Список всех каналов
all_channels as (
    select 'Канатная дорога' as channel union all
    select 'охта парк' union all
    select 'Авито' union all
    select 'Youtube (соц.сеть)' union all
    select 'Инстаграм (соц.сеть)' union all
    select 'Rutube' union all
    select 'Одноклассники' union all
    select 'Групповая Экскурсия' union all
    select 'вакансия на хх' union all
    select 'Афиша Timepad' union all
    select 'Забыли спросить у клиента' union all
    select 'Карты 2gis' union all
    select 'Клиент не помнит' union all
    select 'Наружная реклама' union all
    select 'Посоветовали друзья' union all
    select 'Google (поисковая система)' union all
    select 'Листовки' union all
    select 'Пинтерест (соц.сеть)' union all
    select 'Проходили мимо' union all
    select 'Сайт парка (в котором находится наш объект)' union all
    select 'Тикток (соц.сеть)' union all
    select 'Яндекс (поисковая система)' union all
    select 'Яндекс.Карты' union all
    select 'Вконтакте (соц.сеть)' union all
    select 'Пришли повторно' union all
    select 'Промокод Блогер' union all
    select 'Клиент отказался отвечать' union all
    select 'Смс' union all
    select 'Гугл.Карты' union all
    select 'Телеграм' union all
    select 'Итого' as channel  -- Добавляем канал Итого
),

-- Все города из вашей таблицы + Итого
all_cities as (
    select distinct name_ru as city_ru, name_en as city_en
    from dds.cities
    union all
    select 'Итого' as city_ru, 'Итого' as city_en
),

-- Все периоды из sales_ads_mapping (ограничиваем до текущая неделя - 1)
all_periods as (
    select distinct "year", week
    from dds.sales
    where ("year", week) <= (
        select 
            extract(year from current_date) as year,
            extract(week from current_date) as week
    )
),

-- Создаем полную матрицу всех комбинаций
all_combinations as (
    select 
        p."year",
        p.week,
        c.city_ru,
        c.city_en,
        ch.channel
    from all_periods p
    cross join all_cities c
    cross join all_channels ch
),

dm_totals as (
    select
        ac.year as fas_year,
        ac.week as fas_week,
        ac.city_ru as fas_city,
        ac.channel as fas_channel,
        -- Основные метрики канала (COALESCE для замены NULL на 0)
        
        -- Вся выручка
        CASE 
            WHEN ac.channel = 'Итого' THEN 
                aaat.total_amount
            ELSE
                COALESCE(fas.total_tickets, 0) * aaat.sr_amount
        END  as fas_total_amount,
        
        
        CASE 
            WHEN ac.channel = 'Итого' THEN 
                aaat.offline_tickets 
            ELSE
                COALESCE(fas.total_tickets, 0)
        END  as fas_total_tickets,
        COALESCE(fas.total_ads_qty, 0) as fas_total_ads_qty,
        
        -- Бюджет (COALESCE для замены NULL на 0)
        CASE 
            WHEN ac.channel = 'Итого' THEN 
                COALESCE(max(COALESCE(pd.current_week, 0)) over (partition by ac.year, ac.week, ac.city_ru), 0)
            ELSE
                COALESCE(pd.current_week, 0)
        END as pd_current_week,
        
        -- Агрегированные метрики по городу
        aaat.offline_tickets as total_tickets_city,
        
        aaat.total_amount as total_amount_city,
        
        COALESCE(max(COALESCE(fas.total_ads_qty, 0)) over (partition by ac.year, ac.week, ac.city_ru), 0) as total_ads_qty_city,
        
        COALESCE(max(COALESCE(pd.current_week, 0)) over (partition by ac.city_ru, ac.year, ac.week), 0) as total_budgets_city
        
        
    from all_combinations ac
    left join dds.sales fas on
        ac."year" = fas."year" 
        and ac.week = fas.week 
        and ac.city_en = fas.city 
        and ac.channel = fas.value_group
    left join dds.prepared_budgets pd on
        ac."year" = pd."year"
        and ac.week = pd.week
        and (ac.city_en = pd.city or ac.city_ru = pd.city)
        and ac.channel = pd.channel
    left join all_amount_and_tickets aaat on
        ac."year" = aaat."year"
        and ac.week = aaat.week
        and ac.city_en = aaat.city
),

base_data as (
    select 
        fas_year,
        fas_week,
        fas_city,
        fas_channel,
        fas_total_amount as channel_revenue,
        fas_total_tickets as channel_tickets,
        COALESCE(fas_total_ads_qty, 0) as channel_answers,
        COALESCE(pd_current_week, 0) as channel_budget,
        COALESCE(total_tickets_city, 0) as city_tickets,
        COALESCE(total_amount_city, 0) as city_revenue,
        COALESCE(total_ads_qty_city, 0) as city_answers,
        COALESCE(total_budgets_city, 0) as city_budget,
        CASE 
            WHEN total_tickets_city = 0 THEN 0
            ELSE COALESCE(fas_total_amount / total_tickets_city, 0)
        END as доля_билетов,
        CASE 
            WHEN total_ads_qty_city = 0 THEN 0
            ELSE COALESCE(fas_total_ads_qty / total_ads_qty_city, 0)
        END as доля_ответов,
        COALESCE(total_amount_city / NULLIF(total_tickets_city, 0), 0) as средний_чек_город,
        COALESCE(total_amount_city / NULLIF(total_ads_qty_city, 0), 0) as выручка_на_ответ,
        COALESCE(total_amount_city / NULLIF(pd_current_week, 0), 0) as выполнение_бюджета,
        CASE 
            WHEN fas_channel = 'Итого' THEN COALESCE(total_amount_city / NULLIF(pd_current_week, 0), 0)
            ELSE COALESCE(fas_total_tickets / NULLIF(total_tickets_city, 0), 0) * COALESCE(total_amount_city / NULLIF(pd_current_week, 0), 0)
        END as ROAS
    from dm_totals
),

with_lag_data as (
    select 
        *,
        -- Значения за неделю -1
        LAG(channel_answers, 1) over (partition by fas_city, fas_channel order by fas_year, fas_week) as channel_answers_week_1,
        LAG(channel_budget, 1) over (partition by fas_city, fas_channel order by fas_year, fas_week) as channel_budget_week_1,
        LAG(channel_tickets, 1) over (partition by fas_city, fas_channel order by fas_year, fas_week) as channel_tickets_week_1,
        LAG(city_tickets, 1) over (partition by fas_city, fas_channel order by fas_year, fas_week) as city_tickets_week_1,
        LAG(city_revenue, 1) over (partition by fas_city, fas_channel order by fas_year, fas_week) as city_revenue_week_1,
        
        -- Значения за неделю -2
        LAG(channel_budget, 2) over (partition by fas_city, fas_channel order by fas_year, fas_week) as channel_budget_week_2,
        LAG(channel_tickets, 2) over (partition by fas_city, fas_channel order by fas_year, fas_week) as channel_tickets_week_2,
        LAG(city_tickets, 2) over (partition by fas_city, fas_channel order by fas_year, fas_week) as city_tickets_week_2,
        LAG(city_revenue, 2) over (partition by fas_city, fas_channel order by fas_year, fas_week) as city_revenue_week_2,
        
        -- Значения за неделю -3
        LAG(channel_budget, 3) over (partition by fas_city, fas_channel order by fas_year, fas_week) as channel_budget_week_3,
        LAG(channel_tickets, 3) over (partition by fas_city, fas_channel order by fas_year, fas_week) as channel_tickets_week_3,
        LAG(city_tickets, 3) over (partition by fas_city, fas_channel order by fas_year, fas_week) as city_tickets_week_3,
        LAG(city_revenue, 3) over (partition by fas_city, fas_channel order by fas_year, fas_week) as city_revenue_week_3
        
    from base_data
),

-- Определяем текущую неделю - 1 для сортировки
current_week_minus_1 as (
    select 
        extract(year from current_date - interval '1 week') as year,
        extract(week from current_date - interval '1 week') as week
),

-- Ранжирование на основе только текущей недели - 1
city_ranks_base as (
    select 
        fas_city,
        MAX(city_revenue_week_1) as max_city_revenue_week_1
    from with_lag_data wld
    cross join current_week_minus_1 cw
    where wld.fas_year = cw.year and wld.fas_week = cw.week
    group by fas_city
),

channel_ranks_base as (
    select 
        fas_city,
        fas_channel,
        MAX(channel_revenue) as max_channel_revenue
    from with_lag_data wld
    cross join current_week_minus_1 cw
    where wld.fas_year = cw.year and wld.fas_week = cw.week
    group by fas_city, fas_channel
),

city_ranks as (
    select 
        fas_city,
        ROW_NUMBER() over (
            order by 
                case when fas_city = 'Итого' then 0 else 1 end,
                max_city_revenue_week_1 desc,
                fas_city
        ) as city_rank
    from city_ranks_base
),

channel_ranks as (
    select 
        fas_city,
        fas_channel,
        ROW_NUMBER() over (
            partition by fas_city
            order by 
                case when fas_channel = 'Итого' then 0 else 1 end,
                max_channel_revenue desc,
                fas_channel
        ) as channel_rank
    from channel_ranks_base
),

-- Функция для преобразования числа в формат a1 b2 c3
letter_number_format as (
    select 
        1 as num, 'a_0' as format union all
        select 2, 'a_1' union all
    select 3, 'b_2' union all
    select 4, 'c_3' union all
    select 5, 'd_4' union all
    select 6, 'e_5' union all
    select 7, 'f_6' union all
    select 8, 'g_7' union all
    select 9, 'h_8' union all
    select 10, 'i_9' union all
    select 11, 'j_10' union all
    select 12, 'k_11' union all
    select 13, 'l_12' union all
    select 14, 'm_13' union all
    select 15, 'n_14' union all
    select 16, 'o_15' union all
    select 17, 'p_16' union all
    select 18, 'q_17' union all
    select 19, 'r_18' union all
    select 20, 's_19' union all
    select 21, 't_20' union all
    select 22, 'u_21' union all
    select 23, 'v_22' union all
    select 24, 'w_23' union all
    select 25, 'x_24' union all
    select 26, 'y_25' union all
    select 27, 'z_26' union all
    select 28, 'za_27' union all
    select 29, 'zb_28' union all
    select 30, 'zc_29' union all
    select 31, 'zd_30' union all
    select 32, 'ze_31'
),

ranked_data as (
    select 
        wld.*,
        -- Для городов с одинаковыми названиями используем один ранг, для Итого - 0
        cr.city_rank as city_rank,
        
        -- Для каналов с одинаковыми названиями используем один ранг, для Итого - 0
        chr.channel_rank as channel_rank
        
    from with_lag_data wld
    left join city_ranks cr on
        wld.fas_city = cr.fas_city
    left join channel_ranks chr on
        wld.fas_city = chr.fas_city
        and wld.fas_channel = chr.fas_channel
)

select 
    -- Добавляем номера к названиям городов и каналов в формате a1 b2 c3
    COALESCE(cnf_city.format, city_rank::text) || '. ' || fas_city as ranked_city,
    COALESCE(cnf_channel.format, channel_rank::text) || '. ' || fas_channel as ranked_channel,
    fas_year,
    fas_week,
    channel_revenue,
    channel_tickets,
    channel_answers,
    COALESCE(channel_answers_week_1, 0) as channel_answers_week_1,
    channel_budget,
    city_tickets,
    city_revenue,
    city_answers,
    city_budget,
    доля_билетов,
    доля_ответов,
    средний_чек_город,
    выручка_на_ответ,
    выполнение_бюджета,
    
    -- Значения за предыдущие недели
    COALESCE(channel_budget_week_1, 0) as channel_budget_week_1,
    COALESCE(channel_tickets_week_1, 0) as channel_tickets_week_1,
    COALESCE(city_tickets_week_1, 0) as city_tickets_week_1,
    COALESCE(city_revenue_week_1, 0) as city_revenue_week_1,

    COALESCE(channel_budget_week_2, 0) as channel_budget_week_2,
    COALESCE(channel_tickets_week_2, 0) as channel_tickets_week_2,
    COALESCE(city_tickets_week_2, 0) as city_tickets_week_2,
    COALESCE(city_revenue_week_2, 0) as city_revenue_week_2,
    
    COALESCE(channel_budget_week_3, 0) as channel_budget_week_3,
    COALESCE(channel_tickets_week_3, 0) as channel_tickets_week_3,
    COALESCE(city_tickets_week_3, 0) as city_tickets_week_3,
    COALESCE(city_revenue_week_3, 0) as city_revenue_week_3,
    
    ROAS,
    COALESCE(
        (
            COALESCE(channel_tickets, 0) + COALESCE(channel_tickets_week_1, 0)
        ) / NULLIF(
            COALESCE(city_tickets, 0) + COALESCE(city_tickets_week_1, 0), 
            0
        ), 
        0
    ) * COALESCE(
        (
            COALESCE(city_revenue_week_1, 0) + COALESCE(city_revenue, 0)
        ) / NULLIF(
            COALESCE(channel_budget_week_1, 0) + COALESCE(channel_budget, 0), 
            0
        ), 
        0
    ) as ROAS_2,
    
    COALESCE(
        (
            COALESCE(channel_tickets, 0) + 
            COALESCE(channel_tickets_week_1, 0) + 
            COALESCE(channel_tickets_week_2, 0) + 
            COALESCE(channel_tickets_week_3, 0)
        ) / NULLIF(
            COALESCE(city_tickets, 0) + 
            COALESCE(city_tickets_week_1, 0) + 
            COALESCE(city_tickets_week_2, 0) + 
            COALESCE(city_tickets_week_3, 0), 
            0
        ), 
        0
    ) * COALESCE(
        (
            COALESCE(city_revenue_week_1, 0) + 
            COALESCE(city_revenue, 0) + 
            COALESCE(city_revenue_week_2, 0) + 
            COALESCE(city_revenue_week_3, 0)
        ) / NULLIF(
            COALESCE(channel_budget_week_1, 0) + 
            COALESCE(channel_budget, 0) + 
            COALESCE(channel_budget_week_2, 0) + 
            COALESCE(channel_budget_week_3, 0), 
            0
        ), 
        0
    ) as ROAS_4
    
from ranked_data
left join letter_number_format cnf_city on
    ranked_data.city_rank = cnf_city.num
left join letter_number_format cnf_channel on
    ranked_data.channel_rank = cnf_channel.num
-- Ограничение до текущая неделя - 1
where (fas_year, fas_week) <= (
    select 
        extract(year from current_date) as year,
        extract(week from current_date) as week
)
order by 
    fas_year desc, 
    fas_week desc, 
    -- Сортировка: обычные города по рангу (определенному на основе текущей недели - 1), затем Итого
    case when fas_city = 'Итого' then 0 else 1 end,
    city_rank,
    -- Сортировка: обычные каналы по рангу (определенному на основе текущей недели - 1), затем Итого
    case when fas_channel = 'Итого' then 0 else 1 end,
    channel_rank