/*
  Modèle      : dim_sectors
  Source      : prime-force-478609-s4.silver_linki.int_donnees_geo
  Cible       : prime-force-478609-s4.gold_linki.dim_sectors

  Description :
    Dimension des secteurs d'activité de l'audience LinkedIn. Filtre sur la catégorie 'Secteurs'.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_value AS id_sector,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    valeur      AS sectors,
    pourcentage
FROM {{ ref('int_donnees_geo') }}
WHERE zones_geographiques = 'Secteurs'
