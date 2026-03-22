WITH posts AS (
    SELECT
        id_post AS post_id,
        url_post,
        date_publication_post,
        impressions
    FROM {{ ref('int_posts') }}
),

interactions AS (
    SELECT
        url_post,
        date_publication_post,
        interactions
    FROM {{ ref('int_interactions') }}
)

SELECT
    p.post_id,
    p.url_post,
    p.date_publication_post,
    p.impressions,
    COALESCE(i.interactions, 0)     AS interactions
FROM posts p
LEFT JOIN interactions i
    ON p.url_post = i.url_post
    AND p.date_publication_post = i.date_publication_post