{{ config(
    materialized='incremental',
    unique_key=['post_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH posts AS (
    SELECT
        id_post,
        url_post,
        date_publication_post,
        impressions
    FROM {{ ref('int_posts') }}
    {% if is_incremental() %}
      WHERE date_publication_post > (SELECT COALESCE(MAX(date_publication_post), CAST('1900-01-01' AS DATE)) FROM {{ this }})
    {% endif %}
),

interactions AS (
    SELECT
        url_post,
        date_publication_post,
        interactions
    FROM {{ ref('int_interactions') }}
)

SELECT
    p.id_post AS post_id,
    p.url_post,
    p.date_publication_post,
    p.impressions,
    COALESCE(i.interactions, 0)     AS interactions,
    CURRENT_TIMESTAMP()             AS _at_load
FROM posts p
LEFT JOIN interactions i
    ON p.url_post = i.url_post
    AND p.date_publication_post = i.date_publication_post