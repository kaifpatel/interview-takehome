-- models/silver/int_variant.sql
with deduped as (
    select
        pid::text,
        variant::text,
        actual_size::text,
        weave_feature::text,
        weave_cat::text,
        size_grp::text,
        shipping_size::text,
        shape::text,
        weight::numeric,
        price::numeric,
        msrp::numeric,
        stock_level::integer,
        depletion_level::integer,
        low_stock::boolean,
        estimated_delivery_date::text,
        this_isd_range::text,
        status::text,
        origin::text,
        new_arrival::boolean,
        "stockMsg"::text as stock_msg,
        "stockEddMsg"::text as stock_edd_msg,
        other_stock_core::integer,
        other_stock_compass::integer,
        dw_insert_timestamp::timestamp,
        row_number() over (partition by pid, variant order by dw_insert_timestamp desc) as row_number
    from {{ ref('variant') }}
)
select
    pid,
    variant,
    actual_size,
    weave_feature,
    weave_cat,
    size_grp,
    shipping_size,
    shape,
    weight,
    price,
    msrp,
    stock_level,
    depletion_level,
    low_stock,
    estimated_delivery_date,
    this_isd_range,
    status,
    origin,
    new_arrival,
    stock_msg,
    stock_edd_msg,
    other_stock_core,
    other_stock_compass,
    dw_insert_timestamp
from deduped
where row_number = 1

-- source data 38800 rows, deduped data 38569 rows