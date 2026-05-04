/*
  Modèle      : stg_interactions
  Source      : prime-force-478609-s4.google_drive.contenu_2025_01_21_2026_01_20_ben_mbairo_interactions
                prime-force-478609-s4.google_drive.aggregate_analytics_ben_mbairo_2025_04_16_2026_04_15_interactions
  Cible       : prime-force-478609-s4.bronze_linki.stg_interactions

  Description :
    Historisation des interactions LinkedIn (likes, commentaires, partages) par post.
    Extraction du id_post depuis l'URL pour stabiliser l'identifiant entre exports.
    UNION ALL de plusieurs exports périodiques. Génération de la surrogate key sur le id_post.
*/

WITH export_2025_01_21_2026_01_20 AS (
    SELECT
        LEFT(REGEXP_EXTRACT(url_du_post, r'(\d{10,})'), 8) AS id_post,
        url_du_post,
        date_de_publication_du_post,
        interactions,
        '2025_01_21_2026_01_20'                            AS _periode,
        CURRENT_TIMESTAMP()                                AS _at_load
    FROM {{ source('google_drive', 'contenu_2025_01_21_2026_01_20_ben_mbairo_interactions') }}
),

export_2025_04_16_2026_04_15 AS (
    SELECT
        LEFT(REGEXP_EXTRACT(url_du_post, r'(\d{10,})'), 8) AS id_post,
        url_du_post,
        date_de_publication_du_post,
        interactions,
        '2025_04_16_2026_04_15'                            AS _periode,
        CURRENT_TIMESTAMP()                                AS _at_load
    FROM {{ source('google_drive', 'aggregate_analytics_ben_mbairo_2025_04_16_2026_04_15_interactions') }}
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
    url_du_post,
    date_de_publication_du_post,
    interactions,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _periode,
    _at_load
FROM unioned
