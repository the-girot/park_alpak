INSERT INTO public.offline_sales (
        дата,
        "Покупатель", 
        "Категория номенклатуры", 
        "Номенклатура", 
        "Ед.", 
        "Количество", 
        "Выручка, ₽", 
        city,
        load_dttm
    )
SELECT sale_date, '', item_category, item, '', qty, amount,    case
when kkm = 'ККТ с передачей в ОФД (Эльбрус)' then 'Elbrus_region'
when kkm = 'ККМ (Ростов)' then 'Rostov_On_Don'
when kkm = 'ККТ с передачей в ОФД (Омск)' then 'Omsk'
when kkm = 'ККТ с передачей в ОФД (Москва)' then 'Vorobyovy_Gory'
when kkm = 'ККТ с передачей в ОФД (Уфа)' then 'Ufa'
when kkm = 'ККТ с передачей в ОФД (Волгоград)' then 'Volgograd'
when kkm = 'ККТ с передачей в ОФД (ЕКБ)' then 'Ekaterinburg'
when kkm = 'ККТ с передачей в ОФД (Тюмень)' then 'Tumen'
when kkm = 'ККТ с передачей в ОФД (СПБ)' then 'Saint_Petersburg'
when kkm = 'ККТ с передачей в ОФД (Яхрома)' then 'Moscow'
when kkm = 'ККТ с передачей в ОФД (Казань)' then 'Kazan'
end as kkm, load_dttm
FROM raw.receipts
where "sale_date"::date = '2025-11-05'