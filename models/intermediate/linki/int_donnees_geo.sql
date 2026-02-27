SELECT
    principales_donnees_demographiques  AS zones_geographiques,
    SAFE_CAST(pourcentage AS NUMERIC)   AS pourcentage,
    valeur,
    CURRENT_TIMESTAMP()                 AS _at_load

FROM {{ ref('stg_donnees_geo') }}