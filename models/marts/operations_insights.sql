-- models/marts/operations_insights.sql
SELECT
    v.variant,
    v.actual_size,
    v.stock_level,
    v.depletion_level,
    pu.size,
    pu.shape,
    pu.stock,
    pu.dw_insert_timestamp
FROM
    {{ ref('int_variant')}} v
LEFT JOIN
    {{ ref('pads_upsell')}} pu ON v.variant = pu.variant AND v.pid = pu.p_id
WHERE
    v.stock_level > 0
ORDER BY
    v.depletion_level ASC, v.stock_level DESC