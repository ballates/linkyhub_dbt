/*
  Modèle      : stg_abonnes
  Source      : prime-force-478609-s4.google_drive.contenu_2025_01_21_2026_01_20_ben_mbairo_abonnes
                prime-force-478609-s4.google_drive.aggregate_analytics_ben_mbairo_2025_04_16_2026_04_15_abonnes
  Cible       : prime-force-478609-s4.bronze_linki.stg_abonnes

  Description :
    Historisation des données d'abonnés LinkedIn issues de Google Drive via Fivetran.
    UNION ALL de plusieurs exports périodiques. Déduplication déléguée à la couche intermediate.
    Génération de la surrogate key sur la date.
*/

WITH export_2025_01_21_2026_01_20 AS (
    SELECT
        date_,
        nouveaux_abonnes,
        '2025_01_21_2026_01_20' AS _periode,
        CURRENT_TIMESTAMP()     AS _at_load
    FROM {{ source('google_drive', 'contenu_2025_01_21_2026_01_20_ben_mbairo_abonnes') }}
),

export_2025_04_16_2026_04_15 AS (
    SELECT
        date                    AS date_,
        nouveaux_abonnes,
        '2025_04_16_2026_04_15' AS _periode,
        CURRENT_TIMESTAMP()     AS _at_load
    FROM {{ source('google_drive', 'aggregate_analytics_ben_mbairo_2025_04_16_2026_04_15_abonnes') }}
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
    {{ dbt_utils.generate_surrogate_key(['date_']) }}   AS id_abonne,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    date_,
    nouveaux_abonnes,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _periode,
    _at_load
FROM unioned
