-- models/silver/int_color.sql
with deduped as (
    select
        pid::text,
        color_id::text,
        color_value::text,
        dw_insert_timestamp::timestamp,
        row_number() over (partition by pid, color_id order by dw_insert_timestamp desc) as row_num
    from {{ ref('color_map') }}
)
select
    pid,
    color_id,
    color_value,
    dw_insert_timestamp
from deduped
where row_num = 1

-- source data 17232 rows, deduped data 5640 rows