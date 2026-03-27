SELECT
  id_value AS id_area,
  valeur AS areas,
  pourcentage
FROM {{ref('int_donnees_geo')}}
WHERE zones_geographiques = 'Lieux' 
