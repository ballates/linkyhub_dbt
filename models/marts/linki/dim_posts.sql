SELECT DISTINCT
    FARM_FINGERPRINT(url_post)  AS post_key,
    url_post,
    date_publication_post
FROM {{ ref('int_posts') }}