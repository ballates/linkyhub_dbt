SELECT
    principales_donnees_demographiques  AS zones_geographiques,
    FARM_FINGERPRINT(CONCAT(principales_donnees_demographiques, '|', valeur)) AS id_value,
    SAFE_CAST(pourcentage AS NUMERIC)   AS pourcentage,
    valeur,
    _at_load
FROM {{ ref('stg_donnees_geo') }}
WHERE SAFE_CAST(pourcentage AS NUMERIC) IS NOT NULL