SELECT
    FARM_FINGERPRINT(CONCAT(zones_geographiques, COALESCE(valeur, ''))) AS geo_key,
    zones_geographiques,
    pourcentage,
    valeur
FROM {{ ref('int_donnees_geo') }}