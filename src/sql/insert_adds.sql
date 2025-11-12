    INSERT INTO public.ads_channels_stat (
        "Дата", "Город", "Карты 2gis", "Яндекс.Карты", "Гугл.Карты", 
        "Google (поисковая система)", "Яндекс (поисковая система)", 
        "Инстаграм (соц.сеть)", "Тикток (соц.сеть)", "Вконтакте (соц.сеть)", 
        "Телеграм (мессенджер)", "Пинтерест (соц.сеть)", "Youtube (соц.сеть)", 
        "Туристический сайт", "Сайт парка (в котором находится на", 
        "Афиша Timepad", "Телевидение", "Наружная реклама", "Листовки", 
        "Проходили мимо", "Смс", "Посоветовали друзья", "Пришли повторно", 
        "Клиент не помнит", "Клиент отказался отвечать", "Забыли спросить у клиента"
    )
    WITH unique_data AS (
        SELECT *, 
               ROW_NUMBER() OVER(PARTITION BY  date, anh.city, value ORDER BY load_dttm DESC) as rnk
        FROM raw.ads_new_hash_new anh
        left join dds.cities c on anh.city = c.name_ru
    ), 
    grouped_data AS (
        SELECT 
            date::date AS "Дата",
            name_en AS "Город",
            SUM(CASE WHEN value = 'Карты 2gis' THEN qty ELSE 0 END) AS "Карты 2gis",
            SUM(CASE WHEN value = 'Яндекс.Карты' THEN qty ELSE 0 END) AS "Яндекс.Карты",
            SUM(CASE WHEN value = 'Гугл.Карты' THEN qty ELSE 0 END) AS "Гугл.Карты",
            SUM(CASE WHEN value = 'Google (поисковая система)' THEN qty ELSE 0 END) AS "Google (поисковая система)",
            SUM(CASE WHEN value = 'Яндекс (поисковая система)' THEN qty ELSE 0 END) AS "Яндекс (поисковая система)",
            SUM(CASE WHEN value = 'Инстаграм (соц.сеть)' THEN qty ELSE 0 END) AS "Инстаграм (соц.сеть)",
            SUM(CASE WHEN value = 'Тикток (соц.сеть)' THEN qty ELSE 0 END) AS "Тикток (соц.сеть)",
            SUM(CASE WHEN value = 'Вконтакте (соц.сеть)' THEN qty ELSE 0 END) AS "Вконтакте (соц.сеть)",
            SUM(CASE WHEN value = 'Телеграм (мессенджер)' THEN qty ELSE 0 END) AS "Телеграм (мессенджер)",
            SUM(CASE WHEN value = 'Пинтерест (соц.сеть)' THEN qty ELSE 0 END) AS "Пинтерест (соц.сеть)",
            SUM(CASE WHEN value = 'Youtube (соц.сеть)' THEN qty ELSE 0 END) AS "Youtube (соц.сеть)",
            SUM(CASE WHEN value = 'Туристический сайт' THEN qty ELSE 0 END) AS "Туристический сайт",
            SUM(CASE WHEN value = 'Сайт парка (в котором находится наш объект)' THEN qty ELSE 0 END) AS "Сайт парка (в котором находится на",
            SUM(CASE WHEN value = 'Афиша Timepad' THEN qty ELSE 0 END) AS "Афиша Timepad",
            SUM(CASE WHEN value = 'Телевидение' THEN qty ELSE 0 END) AS "Телевидение",
            SUM(CASE WHEN value = 'Наружная реклама' THEN qty ELSE 0 END) AS "Наружная реклама",
            SUM(CASE WHEN value = 'Листовки' THEN qty ELSE 0 END) AS "Листовки",
            SUM(CASE WHEN value = 'Проходили мимо' THEN qty ELSE 0 END) AS "Проходили мимо",
            SUM(CASE WHEN value = 'Смс' THEN qty ELSE 0 END) AS "Смс",
            SUM(CASE WHEN value = 'Посоветовали друзья' THEN qty ELSE 0 END) AS "Посоветовали друзья",
            SUM(CASE WHEN value = 'Пришли повторно' THEN qty ELSE 0 END) AS "Пришли повторно",
            SUM(CASE WHEN value = 'Клиент не помнит' THEN qty ELSE 0 END) AS "Клиент не помнит",
            SUM(CASE WHEN value = 'Клиент отказался отвечать' THEN qty ELSE 0 END) AS "Клиент отказался отвечать",
            SUM(CASE WHEN value = 'Забыли спросить у клиента' THEN qty ELSE 0 END) AS "Забыли спросить у клиента"
        FROM unique_data
        WHERE rnk = 1
        GROUP BY date::date, name_en
    )
    SELECT 
        "Дата", "Город", "Карты 2gis", "Яндекс.Карты", "Гугл.Карты", 
        "Google (поисковая система)", "Яндекс (поисковая система)", 
        "Инстаграм (соц.сеть)", "Тикток (соц.сеть)", "Вконтакте (соц.сеть)", 
        "Телеграм (мессенджер)", "Пинтерест (соц.сеть)", "Youtube (соц.сеть)", 
        "Туристический сайт", "Сайт парка (в котором находится на", 
        "Афиша Timepad", "Телевидение", "Наружная реклама", "Листовки", 
        "Проходили мимо", "Смс", "Посоветовали друзья", "Пришли повторно", 
        "Клиент не помнит", "Клиент отказался отвечать", "Забыли спросить у клиента"
    FROM grouped_data gd
    
    WHERE "Дата" > '2025-11-05'NOT EXISTS (
        SELECT 1 
        FROM public.ads_channels_stat os 
        WHERE os."Дата" = gd."Дата"
        AND os."Город" = gd."Город"
    );