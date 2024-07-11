-- models/silver/int_links.sql
with deduped as (
    select
        base_url::text,
        page_links::text,
        card_links::text,
        request_time_stamp::timestamp,
        dw_insert_timestamp::timestamp,
        row_number() over (partition by base_url, page_links, card_links order by dw_insert_timestamp desc) as row_num
    from {{ ref('links') }}
)
select
    base_url,
    page_links,
    card_links,
    request_time_stamp,
    dw_insert_timestamp
from deduped
where row_num = 1

-- source data 5565 rows, deduped data 5558 rows