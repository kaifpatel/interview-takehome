-- models/silver/int_pads_upsell.sql
with deduped as (
    select
        size::text,
        shape::text,
        price::numeric,
        width::numeric,
        sqft::numeric,
        type::text,
        stock::integer,
        height::numeric,
        variant::text,
        p_id::text,
        pad_id::text,
        dw_insert_timestamp::timestamp,
        row_number() over (partition by p_id, pad_id order by dw_insert_timestamp desc) as row_number
    from {{ ref('pads_upsell') }}
)
select
    size,
    shape,
    price,
    width,
    sqft,
    type,
    stock,
    height,
    variant,
    p_id,
    pad_id,
    dw_insert_timestamp
from deduped
where row_number = 1

-- source data 97154 rows, deduped data 88485 rows