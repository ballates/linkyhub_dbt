/*
  Modèle      : stg_posts
  Source      : prime-force-478609-s4.google_drive.contenu_2025_01_21_2026_01_20_ben_mbairo_posts
                prime-force-478609-s4.google_drive.aggregate_analytics_ben_mbairo_2025_04_16_2026_04_15_posts
  Cible       : prime-force-478609-s4.bronze_linki.stg_posts

  Description :
    Historisation des données de posts LinkedIn issues de Google Drive via Fivetran.
    Extraction du id_post (8 premiers chiffres de l'ID LinkedIn) depuis l'URL pour stabiliser
    l'identifiant entre les différents formats d'export. UNION ALL de plusieurs exports périodiques.
    Génération de la surrogate key sur le id_post.
*/

WITH export_2025_01_21_2026_01_20 AS (
    SELECT
        LEFT(REGEXP_EXTRACT(url_du_post, r'(\d{10,})'), 8) AS id_post,
        impressions,
        url_du_post,
        date_de_publication_du_post,
        '2025_01_21_2026_01_20'                            AS _periode,
        _fivetran_synced                                   AS _at_load
    FROM {{ source('google_drive', 'contenu_2025_01_21_2026_01_20_ben_mbairo_posts') }}
),

export_2025_04_16_2026_04_15 AS (
    SELECT
        LEFT(REGEXP_EXTRACT(url_du_post, r'(\d{10,})'), 8) AS id_post,
        impressions,
        url_du_post,
        date_de_publication_du_post,
        '2025_04_16_2026_04_15'                            AS _periode,
        _fivetran_synced                                   AS _at_load
    FROM {{ source('google_drive', 'aggregate_analytics_ben_mbairo_2025_04_16_2026_04_15_posts') }}
),

unioned AS (
    SELECT * FROM export_2025_01_21_2026_01_20
    UNION ALL
    SELECT * FROM export_2025_04_16_2026_04_15
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['id_post']) }}     AS id_post,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    impressions,
    url_du_post,
    date_de_publication_du_post,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _periode,
    _at_load
FROM unioned
