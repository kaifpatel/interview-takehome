-- models/silver/int_parent.sql
with deduped as (
    select
        pid::text,
        product_type_id::text,
        name::text,
        url::text,
        origin::text,
        thickness::text,
        material::text,
        weave::text,
        weave_feature::text,
        color::text,
        brand::text,
        "imageName"::text as image_name,
        "imageType"::text as image_type,
        "internalName"::text as internal_name,
        category::text,
        min_price::numeric,
        max_price::numeric,
        availability::text,
        aggregate::boolean,
        clearance::boolean,
        long_description::text,
        shopbyroom::text,
        dw_insert_timestamp::timestamp,
        row_number() over (partition by pid, name order by dw_insert_timestamp desc) as row_number
    from {{ ref('parent') }}
)
select
    pid,
    product_type_id,
    name,
    url,
    origin,
    thickness,
    material,
    weave,
    weave_feature,
    color,
    brand,
    image_name,
    image_type,
    internal_name,
    category,
    min_price,
    max_price,
    availability,
    aggregate,
    clearance,
    long_description,
    shopbyroom,
    dw_insert_timestamp
from deduped
where row_number = 1

-- source data 5562 rows, deduped data 3404 rows