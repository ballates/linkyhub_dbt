SELECT
  valeur AS areas,
  pourcentage
FROM {{ref('int_donnees_geo')}}
WHERE zones_geographiques = 'Lieux' 
