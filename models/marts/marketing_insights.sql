-- models/marts/marketing_insights.sql
WITH extracted_pid AS (
    SELECT
        *,
        substring(card_links from '/([^/]+)-P.html') AS pid
    FROM
        {{ ref('int_links') }}
)

SELECT
    cm.category_value,
    c.color_value,
    COUNT(DISTINCT l.page_links) AS page_views,
    COUNT(DISTINCT l.card_links) AS card_views
FROM
    {{ ref('int_category') }} cm
JOIN
    {{ ref('int_color') }} c ON cm.pid = c.pid
JOIN
    extracted_pid l ON cm.pid = l.pid
GROUP BY
    cm.category_value, c.color_value
ORDER BY
    page_views DESC, card_views DESC