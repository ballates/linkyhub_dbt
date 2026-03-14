SELECT
 valeur AS sectors,
 pourcentage
FROM {{ref('int_donnees_geo')}}
WHERE zones_geographiques = 'Secteurs'
