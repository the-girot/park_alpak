with prepared_data as (
select
    created_at::date,
    price_nominal - timepad_commission - acquiring_commission as amount,
--    case 
--            when ticket_type_name like '%орм%' then 0 
--            else 1 
--        end as is_ticket,
    c.name_en
from public.online_sales_simple oss
join dds.cities c on oss.event_id = c.event_id
where status_name in ('paid','transfer_payment')


union all

select
    created_at::date,
    sum(price_nominal - timepad_commission - acquiring_commission),
--    sum(case 
--            when ticket_type_name like '%орм%' then 0 
--            else 1 
--        end) as is_ticket,
    'Итого'
from public.online_sales_simple oss
join dds.cities c on oss.event_id = c.event_id
where status_name in ('paid','transfer_payment')
group by created_at::date, ticket_type_name

union all

select
    r.sale_date::date,
   r.amount ,
    --CASE WHEN r.item_category = 'Билеты' THEN r.qty  ELSE 0 END AS total_tickets,
    c.name_en 
from raw.receipts r
join dds.cities c on
    c.kkm_name = r.kkm

union all 
select
    r.sale_date::date,
    sum(r.amount),
    --SUM(CASE WHEN r.item_category = 'Билеты' THEN r.qty  ELSE 0 END) AS total_tickets,
    'Итого'
from raw.receipts r
group by 
r.sale_date::date
)
select 
    extract(year from created_at) as year,
    extract(week from created_at) as week,
    name_en as city,
    sum(amount)
    --sum(is_ticket)
from prepared_data
group by 
    extract(year from created_at),
    extract(week from created_at),
    name_en