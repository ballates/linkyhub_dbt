SELECT
 valeur AS type_company,
 pourcentage
FROM  {{ref('int_donnees_geo')}}
WHERE zones_geographiques = 'Intitulés de poste'
