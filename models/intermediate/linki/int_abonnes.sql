/*
  Modèle      : int_abonnes
  Source      : prime-force-478609-s4.bronze_linki.stg_abonnes
  Cible       : prime-force-478609-s4.silver_linki.int_abonnes

  Description :
    Parsing de la date (formats variés). Déduplication par date :
    on conserve uniquement l'export le plus récent (_periode DESC).
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_abonne,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    COALESCE(
        SAFE_CAST(date_ AS DATE),
        SAFE.PARSE_DATE('%d/%m/%Y', CAST(date_ AS STRING))
    )                                   AS date,
    nouveaux_abonnes,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _at_load
FROM {{ ref('stg_abonnes') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY date_
    ORDER BY SAFE.PARSE_DATE('%Y_%m_%d', RIGHT(_periode, 10)) DESC
) = 1
