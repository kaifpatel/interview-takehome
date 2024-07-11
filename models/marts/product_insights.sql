-- models/marts/product_insights.sql
SELECT
    p.pid,
    p.name,
    v.variant,
    v.actual_size,
    v.price,
    v.stock_level,
    v.depletion_level,
    pu.size,
    pu.shape,
    pu.price AS upsell_price
FROM
    {{ ref('parent')}} p
JOIN
    {{ ref('variant')}} v ON p.pid = v.pid
LEFT JOIN
    {{ ref('pads_upsell')}} pu ON v.variant = pu.variant AND v.pid = pu.p_id
ORDER BY
    v.price DESC, v.stock_level DESC