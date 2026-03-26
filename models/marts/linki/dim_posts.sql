SELECT
    id_post,
    url_post,
    date_publication_post,
    CONCAT('Post du ',FORMAT_DATE('%d-%m-%Y', date_publication_post)) AS label_post
FROM {{ ref('int_posts') }}
