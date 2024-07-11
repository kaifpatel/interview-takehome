-- models/gold/category_insights.sql
select
    cm.category_value as category,
    count(distinct v.pid) as product_count,
    round(avg(v.price::numeric), 2) as avg_price
from {{ ref('category_map') }} cm
join {{ ref('variant') }} v on cm.pid = v.pid
group by cm.category_value
order by avg_price desc