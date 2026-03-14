SELECT
 valeur AS hierarchy_level,
 CAST(pourcentage AS DECIMAL) AS percent
FROM {{ref('int_donnees_geo')}}
WHERE zones_geographiques = 'Niveau hiérarchique'
