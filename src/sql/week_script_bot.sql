with all_sales as (
select
'online' as source,
    created_at::date,
    price_nominal - timepad_commission - acquiring_commission as amount,
    c.name_en
from public.online_sales_simple oss
join dds.cities c on oss.event_id = c.event_id
where status_name in ('paid','transfer_payment')


union all

select
'online' as source,
    created_at::date,
    sum(price_nominal - timepad_commission - acquiring_commission),
    'По всем городам'
from public.online_sales_simple
where status_name in ('paid','transfer_payment')
group by created_at::date, ticket_type_name

union all

select
'offline' as source,
    дата::date,
    "Выручка, ₽",
    city
from public.offline_sales

union all 
select
'offline' as source,
    дата::date,
    sum("Выручка, ₽"),
    'По всем городам'
from public.offline_sales
group by 
дата::date
)
select 
    dd.year,
    dd.week,
    dd.week_range,
    name_en,
    sum(amount) as amount
from all_sales as as_
left join public.dim_dates dd on as_.created_at::date = dd.date::date
where name_en != 'Итого' and source='offline'
group by dd.year,
    dd.week,
    dd.week_range,
    name_en

union