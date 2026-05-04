/*
  Modèle      : int_posts
  Source      : prime-force-478609-s4.bronze_linki.stg_posts
  Cible       : prime-force-478609-s4.silver_linki.int_posts

  Description :
    Extraction du titre du post depuis le slug de l'URL (nouveau format LinkedIn uniquement).
    Agrégation par id_post : somme des impressions sur tous les exports, MAX sur les colonnes descriptives
    (MAX sur title_post récupère naturellement la valeur non-nulle du nouveau format d'URL).
*/

WITH base AS (
    SELECT
        id_post,
        REGEXP_REPLACE(
            REPLACE(REGEXP_EXTRACT(url_du_post, r'_(.+)-\d{10,}'), '-', ' '),
            r'\s*(ugcPost|share)\s*$', ''
        )                                                                    AS title_post,
        impressions,
        url_du_post,
        date_de_publication_du_post,
        _at_load
    FROM {{ ref('stg_posts') }}
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_post,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    MAX(title_post)                                                 AS title_post,
    SUM(impressions)                                                AS impressions,
    MAX(url_du_post)                                                AS url_post,
    SAFE.PARSE_DATE('%d/%m/%Y', MAX(date_de_publication_du_post))   AS date_publication_post,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    MAX(_at_load)                                                   AS _at_load
FROM base
GROUP BY id_post
