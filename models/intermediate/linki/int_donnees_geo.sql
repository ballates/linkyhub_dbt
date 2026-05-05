/*
  Modèle      : int_donnees_geo
  Source      : prime-force-478609-s4.bronze_linki.stg_donnees_geo
  Cible       : prime-force-478609-s4.silver_linki.int_donnees_geo

  Description :
    Cast numérique du pourcentage. Filtre sur les lignes sans valeur numérique valide.
    Renommage de la colonne catégorie en zones_geographiques.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_donnee_geo                               AS id_value,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    principales_donnees_demographiques          AS zones_geographiques,
    SAFE_CAST(pourcentage AS NUMERIC)           AS pourcentage,
    valeur,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _at_load
FROM {{ ref('stg_donnees_geo') }}
WHERE SAFE_CAST(pourcentage AS NUMERIC) IS NOT NULL
