/*
  Modèle      : dim_hierarchy_level
  Source      : prime-force-478609-s4.silver_linki.int_donnees_geo
  Cible       : prime-force-478609-s4.gold_linki.dim_hierarchy_level

  Description :
    Dimension des niveaux hiérarchiques de l'audience LinkedIn. Filtre sur la catégorie 'Niveau hiérarchique'.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_value AS id_hierarchy_level,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    valeur      AS hierarchy_level,
    pourcentage
FROM {{ ref('int_donnees_geo') }}
WHERE zones_geographiques = 'Niveau hiérarchique'
