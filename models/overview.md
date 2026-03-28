{% docs __overview__ %}

# Bienvenue sur la documentation LinkyHub — Ben Mbairo

Ce projet dbt transforme les données **LinkedIn** en un pipeline analytique structuré et documenté, hébergé sur **BigQuery**.

---

## Architecture du pipeline

```
Sources :
      
Bronze : bronze_linki   →  Vues staging (données brutes nettoyées)
      
Silver : silver_linki   →  Tables intermédiaires (normalisées, dédupliquées)
      
Gold   : gold_linki     →  Tables analytiques finales (dims & facts)
      
Power BI                →  Rapport de visualisation des KPIs LinkedIn
```

---

## Objectif

L'objectif final de ce pipeline est d'alimenter un **rapport Power BI** permettant de visualiser les KPIs liés aux **impressions** et aux **abonnements** du profil LinkedIn de Ben Mbairo, afin de suivre et d'analyser la performance de sa présence sur la plateforme.

---

## Sources de données

Les données proviennent de :

| Source | Tables |
|---|---|
| LinkedIn (`linki_bucket_set`) | invitations, connections, certifications, formations |
| Google Drive (`google_drive`) | posts, impressions, interactions, abonnés, données démographiques |

---

## Modèles

| Couche | Schéma | Nombre de modèles | Type |
|---|---|---|---|
| Staging | `bronze_linki` | 8 | Vues |
| Intermediate | `silver_linki` | 8 | Tables |
| Marts | `gold_linki` | 11 | Tables / Incrémental |

**Fact tables :** `fct_posts`, `fct_abonnes`

**Dimensions :** `dim_posts`, `dim_connections`, `dim_invitations`, `dim_certifications`, `dim_learning`, `dim_area`, `dim_sectors`, `dim_hierarchy_level`, `dim_calendar`

---

## Qualité des données

Le projet contient **plusieurs tests automatisés** :
- Unicité et non-nullité des clés
- Valeurs acceptées 
- Plages de valeurs numériques
- Intégrité référentielle entre facts et dimensions
- Validation de formats (emails, URLs LinkedIn)

---

## Navigation

- **Project** : explore les modèles par couche (staging → intermediate → marts)
- **Database** : explore les tables par schéma BigQuery
- **Lineage graph** : visualise les dépendances entre modèles (icône bleue en bas à droite)

{% enddocs %}
