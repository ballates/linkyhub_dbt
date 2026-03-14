SELECT
 valeur AS companies,
 pourcentage
FROM {{ref('int_donnees_geo')}}
WHERE zones_geographiques = 'Entreprises'


