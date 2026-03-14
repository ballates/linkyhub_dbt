SELECT
 valeur AS size_companies,
 pourcentage
FROM {{ref('int_donnees_geo')}}
WHERE zones_geographiques = "Taille de l'entreprise"
