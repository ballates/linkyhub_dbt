WITH posts AS (
    SELECT
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
),

abonnes AS (
    SELECT
        date,
        nouveaux_abonnes
    FROM {{ ref('int_abonnes') }}
),

joined AS (
    SELECT
        p.url_post,
        p.date_publication_post,
        p.impressions,
        COALESCE(i.interactions, 0)     AS interactions,
        COALESCE(a.nouveaux_abonnes, 0) AS nouveaux_abonnes
    FROM posts p
    LEFT JOIN interactions i
        ON p.url_post = i.url_post
        AND p.date_publication_post = i.date_publication_post
    LEFT JOIN abonnes a
        ON DATE(p.date_publication_post) = DATE(a.date)
)

SELECT
    FARM_FINGERPRINT(CONCAT(url_post, CAST(date_publication_post AS STRING))) AS post_key,
    url_post,
    date_publication_post,
    impressions,
    interactions,
    nouveaux_abonnes,
    CURRENT_TIMESTAMP() AS _at_load
FROM joined