SELECT
    impressions,
    url_du_post                                                     AS url_post,
    SAFE.PARSE_DATE('%d/%m/%Y', date_de_publication_du_post)        AS date_publication_post,
    CURRENT_TIMESTAMP()                                             AS _at_load
FROM {{ ref('stg_posts') }}