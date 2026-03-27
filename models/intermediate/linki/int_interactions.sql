SELECT
    url_du_post                                                         AS url_post,
    SAFE.PARSE_DATE('%d/%m/%Y', date_de_publication_du_post)            AS date_publication_post,
    interactions,
    _at_load
FROM {{ ref('stg_interactions') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY url_du_post, date_de_publication_du_post ORDER BY interactions DESC) = 1