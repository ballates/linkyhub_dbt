/*
  Modèle      : dim_area
  Source      : prime-force-478609-s4.silver_linki.int_donnees_geo
  Cible       : prime-force-478609-s4.gold_linki.dim_area

  Description :
    Dimension des zones géographiques de l'audience LinkedIn. Filtre sur la catégorie 'Lieux'.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_value AS id_area,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    valeur      AS areas,
    pourcentage
FROM {{ ref('int_donnees_geo') }}
WHERE zones_geographiques = 'Lieux'
