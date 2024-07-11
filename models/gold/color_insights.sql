-- models/gold/color_insights.sql
select
    cm.color_value as color,
    count(distinct v.pid) as product_count,
    round(avg(v.price::numeric), 2) as avg_price
from {{ ref('color_map') }} cm
join {{ ref('variant') }} v on cm.pid = v.pid
group by cm.color_value
order by product_count desc