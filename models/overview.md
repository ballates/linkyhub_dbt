{% docs __overview__ %}

# Bienvenue sur la documentation LinkyHub - Ben Mbairo

Ce projet dbt transforme les données **LinkedIn** en un pipeline analytique structuré et documenté, hébergé sur **BigQuery**.

## Flux complet des données

| Origine | Couche | Schéma | Modèles | Destination |
|---|---|---|---|---|
| `linky_bucket_landing` | Staging | `bronze_linki` | `stg_*` (vues) | ↓ |
| `google_drive` | Staging | `bronze_linki` | `stg_*` (vues) | ↓ |
| — | Intermediate | `silver_linki` | `int_*` (tables) | ↓ |
| — | Marts | `gold_linki` | `fct_*` / `dim_*` (incrémental) | **Power BI** |

| Couche | Schéma | Matérialisation | Rôle |
|---|---|---|---|
| Bronze | `bronze_linki` | Vues | Données brutes nettoyées |
| Silver | `silver_linki` | Tables | Normalisées, dédupliquées |
| Gold | `gold_linki` | Tables / Incrémental | Dims & facts prêts pour Power BI |

---

## Objectif

Le but ici est d'alimenter un **rapport Power BI** permettant de visualiser les KPIs liés aux **impressions** et aux **abonnements** de mon profil LinkedIn, www.linkedin.com/in/be4183al, afin de suivre et d'analyser la performance de ma présence sur la plateforme.

---

## Sources de données

| Source BigQuery | Alimentation | Tables |
|---|---|---|
| `linky_bucket_landing` | Fivetran (GCS `linki_bucket` → BQ) | invitations, connections, certifications, learning, comments, reactions |
| `google_drive` | Fivetran (sync automatique) | posts, interactions, abonnés, données démographiques |

---

## Modèles

| Couche | Schéma | Nombre de modèles | Type |
|---|---|---|---|
| Staging | `bronze_linki` | 10 | Vues |
| Intermediate | `silver_linki` | 9 | Tables |
| Marts | `gold_linki` | 12 | Tables / Incrémental |

**Fact tables :** `fct_posts`, `fct_engagement`, `fct_abonnes`

**Dimensions :** `dim_posts`, `dim_connections`, `dim_invitations`, `dim_certifications`, `dim_learning`, `dim_area`, `dim_sectors`, `dim_hierarchy_level`, `dim_calendar`

**Convention clés :** `id_nom` = clé primaire (PK) — `nom_id` = clé étrangère (FK, ex: `post_id` dans `fct_engagement`)

---

## Clés surrogates

Toutes les dimensions et facts utilisent `dbt_utils.generate_surrogate_key` pour générer des clés uniques. Pour les modèles historisés (`stg_posts`, `stg_interactions`, `stg_abonnes`, `stg_donnees_geo`), les clés sont générées dès la couche staging et propagées telles quelles.

---

## Historisation des exports LinkedIn

LinkedIn exporte les données sur une **fenêtre glissante d'un an**. Chaque export est ajouté via `UNION ALL` en staging. Deux stratégies de consolidation sont appliquées en intermediate :

| Métrique | Stratégie | Raison |
|---|---|---|
| **Impressions** | Pro-rata temporis | Fenêtre glissante → risque de double comptage |
| **Interactions** | Export le plus récent | Métrique de stock, peut baisser (unlike) |
| **Données géo** | Export le plus récent | Snapshot de répartition d'audience, pas une série temporelle |

**Pro-rata temporis :** `contribution = impressions × (jours_nouveaux / 365)`

---

## Traçabilité (`_at_load`)

| Couche | Valeur | Signification |
|---|---|---|
| Staging | `_fivetran_synced` | Quand Fivetran a chargé les données sources |
| Marts | `CURRENT_TIMESTAMP()` | Quand dbt a matérialisé la ligne en table |

---

## Qualité des données

**Plusieurs tests automatisés** couvrant :

- Unicité et non-nullité des clés surrogates (`id_*`)
- Valeurs acceptées
- Plages de valeurs numériques (impressions, interactions, pourcentages)
- Intégrité référentielle entre facts et dimensions
- Validation de formats (emails, URLs LinkedIn)

---

## Power BI

Le rapport Power BI consomme les tables de la couche Gold (`gold_linki`) pour visualiser les KPIs LinkedIn.

**KPIs suivis :**
- Évolution des impressions par post
- Évolution des abonnements dans le temps
- Performance des interactions (likes, commentaires, partages)
- Analyse de l'engagement par post (commentaires et réactions via `fct_engagement`) — croisement avec `interactions` LinkedIn pour estimer l'activité audience
- Analyse démographique des abonnés (zones géographiques, secteurs, niveaux hiérarchiques)

### Relation `dim_posts` ↔ `fct_engagement`

`fct_engagement` couvre **toute l'activité LinkedIn** de Ben Mbairo : ses propres posts **et** les posts des autres membres.

| `is_own_post` | Signification |
|---|---|
| `TRUE` | Post présent dans `dim_posts` (ses posts à lui) |
| `FALSE` | Post d'un autre membre — orphelin dans la relation |

**Stratégie Power BI :** relation active `dim_posts.id_post = fct_engagement.post_id`, filtre systématique sur `is_own_post = TRUE` dans les visuels liés aux posts. Les orphelins (`FALSE`) sont utilisés séparément pour analyser l'activité sur les posts des autres.

**Lecture interactions vs engagement calculé :**
- `fct_posts.nb_comments` + `nb_reactions` = activité propre de Ben Mbairo sous ses posts
- `fct_posts.interactions` = activité totale de l'audience (tous membres confondus)
- Différence = comments + réactions + republications des **autres membres**

![Dashboard Power BI](https://ballates.github.io/linkyhub_dbt/assets/impressions.png)

---

## CI/CD

| Workflow | Déclencheur | Jobs |
|---|---|---|
| `pipeline_ci_cd.yml` | Pull Request vers `main` | `dbt-ci` → `merge` → `dbt-deploy` → `deploy-docs` |
| `auto-trigger.yml` | Lun, Jeu, Sam à 8h | polling BigQuery → build prod si données changées |

Les dépendances sont gérées via **uv** pour des installations rapides sur les runners GitHub.

---

## Navigation

- **Project** : explore les modèles par couche (staging → intermediate → marts)
- **Database** : explore les tables par schéma BigQuery
- **Lineage graph** : visualise les dépendances entre modèles (icône bleue en bas à droite)

{% enddocs %}