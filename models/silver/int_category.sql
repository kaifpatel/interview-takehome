-- models/silver/int_category.sql
with deduped as (
    select
        pid::text,
        category_id::text,
        category_value::text,
        dw_insert_timestamp::timestamp,
        row_number() over (partition by pid, category_id order by dw_insert_timestamp desc) as row_num
    from {{ ref('category_map') }}
)
select
    pid,
    category_id,
    category_value,
    dw_insert_timestamp
from deduped
where row_num = 1
 
 -- further validation is necessary to confirm the nuances with the data type (eg. timestamp timezone), but these data types should work for now.
 -- source category data contains 51,265 rows, but once deduplicated it contains 31,758 rows. thus deduplication is likely a necessary transformation, although business logic will need to be confirmed in order to ascertain whether these deduplication tranformations are correct.