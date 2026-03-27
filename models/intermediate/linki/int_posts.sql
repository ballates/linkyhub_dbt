SELECT
    FARM_FINGERPRINT(url_du_post) AS id_post,
    impressions,
    url_du_post                                                     AS url_post,
    SAFE.PARSE_DATE('%d/%m/%Y', date_de_publication_du_post)        AS date_publication_post,
    _at_load
FROM {{ ref('stg_posts') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY url_du_post, date_de_publication_du_post ORDER BY impressions DESC) = 1