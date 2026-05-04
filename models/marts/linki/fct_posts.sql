/*
  Modèle      : fct_posts
  Source      : prime-force-478609-s4.silver_linki.int_posts
                prime-force-478609-s4.silver_linki.int_interactions
  Cible       : prime-force-478609-s4.gold_linki.fct_posts

  Description :
    Table de faits des posts LinkedIn. Joint les impressions et les interactions par id_post.
    Modèle incrémental : merge sur id_post, nouvelles publications uniquement.
*/

{{ config(
    materialized='incremental',
    unique_key=['id_post'],
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
        id_post,
        interactions
    FROM {{ ref('int_interactions') }}
    {% if is_incremental() %}
      WHERE date_publication_post > (SELECT COALESCE(MAX(date_publication_post), CAST('1900-01-01' AS DATE)) FROM {{ this }})
    {% endif %}
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    p.id_post,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    p.url_post,
    p.date_publication_post,
    p.impressions,
    COALESCE(i.interactions, 0)     AS interactions,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP()             AS _at_load
FROM posts p
LEFT JOIN interactions i
    ON p.id_post = i.id_post
