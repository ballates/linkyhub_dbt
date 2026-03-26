SELECT
 id_value AS id_hierarchy_level,
 valeur AS hierarchy_level,
 pourcentage
FROM {{ref('int_donnees_geo')}}
WHERE zones_geographiques = 'Niveau hiérarchique'
