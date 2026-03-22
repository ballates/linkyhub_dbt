SELECT
 id_value AS id_sector,
 valeur AS sectors,
 pourcentage
FROM {{ref('int_donnees_geo')}}
WHERE zones_geographiques = 'Secteurs'
