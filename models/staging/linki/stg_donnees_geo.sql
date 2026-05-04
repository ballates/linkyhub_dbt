/*
  Modèle      : stg_donnees_geo
  Source      : prime-force-478609-s4.google_drive.contenu_2025_01_21_2026_01_20_ben_mbairo_donnees_demographiques
                prime-force-478609-s4.google_drive.aggregate_analytics_ben_mbairo_2025_04_16_2026_04_15_donnees_demographiques
  Cible       : prime-force-478609-s4.bronze_linki.stg_donnees_geo

  Description :
    Historisation des données démographiques LinkedIn (zones géographiques, secteurs, niveaux hiérarchiques).
    UNION ALL de plusieurs exports périodiques. Génération de la surrogate key sur la catégorie et la valeur.
*/

WITH export_2025_01_21_2026_01_20 AS (
    SELECT
        principales_donnees_demographiques,
        pourcentage,
        valeur,
        '2025_01_21_2026_01_20' AS _periode,
        CURRENT_TIMESTAMP()     AS _at_load
    FROM {{ source('google_drive', 'contenu_2025_01_21_2026_01_20_ben_mbairo_donnees_demographiques') }}
),

export_2025_04_16_2026_04_15 AS (
    SELECT
        principales_donnees_demographiques,
        pourcentage,
        valeur,
        '2025_04_16_2026_04_15' AS _periode,
        CURRENT_TIMESTAMP()     AS _at_load
    FROM {{ source('google_drive', 'aggregate_analytics_ben_mbairo_2025_04_16_2026_04_15_donnees_demographiques') }}
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
    {{ dbt_utils.generate_surrogate_key(['principales_donnees_demographiques', 'valeur']) }} AS id_donnee_geo,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    principales_donnees_demographiques,
    pourcentage,
    valeur,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _periode,
    _at_load
FROM unioned
