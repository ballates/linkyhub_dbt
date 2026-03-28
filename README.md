# LinkyHub dbt — Ben Mbairo

Pipeline analytique dbt transformant les données LinkedIn en KPIs visualisés sur Power BI, hébergé sur BigQuery.

---

## Table des matières

1. [Architecture](#architecture)
2. [Sources de données](#sources-de-données)
3. [Modèles](#modèles)
4. [Qualité des données](#qualité-des-données)
5. [Packages & macros](#packages--macros)
6. [Installation & configuration](#installation--configuration)
7. [Commandes](#commandes)
8. [CI/CD](#cicd)
9. [Bonnes pratiques](#bonnes-pratiques)
10. [Limites & évolutions futures](#limites--évolutions-futures)
11. [Power BI](#power-bi)

---

## Architecture

Les schemas sont séparés par environnement :

| Environnement | Schemas |
|---|---|
| Local (`dev`) | `dev_bronze_linki`, `dev_silver_linki`, `dev_gold_linki` |
| Production (`prod`) | `bronze_linki`, `silver_linki`, `gold_linki` |

### Flux complet des données

```mermaid
flowchart LR
    manuel["🙋 Dépôt manuel"] -->|"upload"| bucket[("linki_bucket_set")]
    fivetran["📥 Fivetran"] -->|"sync"| gdrive[("google_drive")]

    subgraph bq ["🗄️ BigQuery"]
        direction LR
        bucket
        gdrive
        bronze[("🥉 bronze_linki\nvues")]
        silver[("🥈 silver_linki\ntables")]
        gold[("🥇 gold_linki\nfacts & dims")]
    end

    subgraph dbt ["⚙️ dbt"]
        direction LR
        stg["staging\nstg_*"] -->|"normalise"| int["intermediate\nint_*"] -->|"agrège"| mart["marts\nfct_* · dim_*"]
    end

    bucket -->|"lecture"| stg
    gdrive -->|"lecture"| stg
    stg -->|"matérialise vues"| bronze
    int -->|"matérialise tables"| silver
    mart -->|"matérialise incrémental"| gold

    gold -->|"consommation"| powerbi["📊 Power BI"]
```

### Clés surrogates

Toutes les dimensions et facts utilisent `FARM_FINGERPRINT` pour générer des clés uniques :

```sql
FARM_FINGERPRINT(CONCAT(champ1, '|', champ2)) AS id_model
```
---

## Sources de données

Données synchronisées via **Fivetran** :

| Source BigQuery | Tables |
|---|---|
| `linki_bucket_set` | invitations, connections, certifications, learning |
| `google_drive` | posts, interactions, abonnés, données démographiques |

---

## Modèles

| Couche | Schéma | Modèles | Matérialisation |
|---|---|---|---|
| Staging | `bronze_linki` | `stg_posts`, `stg_connections`, `stg_invitations`, `stg_certifications`, `stg_learning`, `stg_abonnes`, `stg_interactions`, `stg_donnees_geo` | Vue |
| Intermediate | `silver_linki` | `int_posts`, `int_connections`, `int_invitations`, `int_certifications`, `int_learning`, `int_abonnes`, `int_interactions`, `int_donnees_geo` | Table |
| Marts | `gold_linki` | `fct_posts`, `fct_abonnes`, `dim_posts`, `dim_connections`, `dim_invitations`, `dim_certifications`, `dim_learning`, `dim_area`, `dim_sectors`, `dim_hierarchy_level`, `dim_calendar` | Table / Incrémental |

**Fact tables :** `fct_posts` et `fct_abonnes` sont en mode incrémental (`merge` strategy).

---

## Qualité des données

146 tests automatisés couvrant :

- **Unicité** — clés surrogates (`id_*`) sur tous les modèles
- **Non-nullité** — colonnes critiques
- **Valeurs acceptées** — ex: direction `OUTGOING` / `INCOMING`
- **Plages numériques** — impressions, interactions, pourcentages
- **Intégrité référentielle** — facts → dimensions
- **Format** — emails et URLs LinkedIn (regex)

---

## Packages & macros

### Packages externes

| Package | Version | Utilisation |
|---|---|---|
| `dbt-labs/dbt_utils` | 1.1.1 | Utilitaires SQL : génération de clés surrogates, tests génériques |
| `metaplane/dbt_expectations` | 0.10.10 | Tests avancés : plages de valeurs, types de données, expressions régulières |

### Macros custom

#### `normalize_email`
Normalise une adresse email en minuscules et supprime les espaces.
```sql
{{ normalize_email('email_address') }}
-- équivalent à : LOWER(TRIM(email_address))
```
Utilisée dans : `int_connections`, `dim_connections`

#### `generate_date_spine`
Génère une séquence de dates entre deux bornes via `GENERATE_DATE_ARRAY` BigQuery.
```sql
{{ generate_date_spine('2021-01-01', '2027-12-31') }}
```
Utilisée dans : `dim_calendar`

#### `generate_schema_name`
Contrôle le nom du schema selon l'environnement.
- En `dev` → préfixe `dev_` (ex: `dev_bronze_linki`)
- En `prod` → nom direct (ex: `bronze_linki`)

---

## Installation & configuration

### Prérequis

- [uv](https://github.com/astral-sh/uv) (gestionnaire de paquets Python)
- Accès BigQuery (projet `prime-force-478609-s4`)
- Service account JSON GCP

### Installation

Ce projet utilise [uv](https://github.com/astral-sh/uv) pour la gestion de l'environnement virtuel et des dépendances.

```bash
# Installer uv (si pas déjà installé)
pip install uv

# Créer l'environnement virtuel et installer les dépendances
uv venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

uv pip install "dbt-bigquery>=1.11.0"

# Installer les packages dbt
dbt deps
```

### Configuration

Le projet utilise le `profiles.yml` à la racine (`--profiles-dir .`). Le keyfile est résolu automatiquement :

- En **local** : chemin par défaut vers `.env/prime-force-478609-s4-*.json` (pas de variable à définir)
- En **CI/CD** : la variable `DBT_KEYFILE` est injectée par GitHub Actions via le secret `GCP_SERVICE_ACCOUNT_KEY`

Place ton fichier service account JSON dans `.env/` à la racine du projet. Le `profiles.yml` le récupère via :

```yaml
keyfile: "{{ env_var('DBT_KEYFILE', '/chemin/vers/.env/service-account.json') }}"
```

---

## Commandes

### Développement local

```bash
# Compiler le SQL (vérifie la syntaxe sans toucher BigQuery)
dbt compile

# Builder tous les modèles + tests
dbt build

# Builder un modèle spécifique + ses dépendants
dbt build --select fct_posts+

# Builder uniquement la couche staging
dbt build --select staging

# Lancer uniquement les tests
dbt test

# Recréer les modèles incrémentaux from scratch
dbt build --full-refresh

# Nettoyer les fichiers compilés
dbt clean

# Générer et ouvrir la documentation en local
dbt docs generate
dbt docs serve
```

### CI/CD (GitHub Actions)

```bash
# CI — validation sur Pull Request
dbt compile --profiles-dir . --target dev
dbt build --profiles-dir . --target dev

# Deploy — déploiement en production
dbt build --profiles-dir . --target prod
dbt docs generate --profiles-dir . --target prod --select "path:models"
```

---

## CI/CD

Deux workflows GitHub Actions :

| Workflow | Déclencheur | Jobs |
|---|---|---|
| `pipeline_ci_cd.yml` | Pull Request vers `main` | `dbt-ci` → `merge` → `dbt-deploy` → `deploy-docs` |
| `auto-trigger.yml` | Lun, Jeu, Sam à 8h | polling BigQuery → build prod si données changées |

### Vue d'ensemble

```mermaid
flowchart LR
    dev[Dev] -->|"git push + PR"| branch[feature/ben]

    subgraph pipeline ["pipeline_ci_cd.yml — PR vers main"]
        direction LR
        c1[dbt-ci\ncompile + build + tests] -->|"CI passe"| c2[merge\nPR squash] -->|"merge OK"| c3[dbt-deploy\nbuild prod + docs] --> c4[deploy-docs\nGitHub Pages]
        c1 -->|"echec"| block[PR bloquee]
    end

    subgraph trigger ["auto-trigger.yml — Lun/Jeu/Sam 8h"]
        bq[(BigQuery)] -->|"polling"| t2{BQ modifie ?}
        t2 -->|"OUI"| build[dbt build prod]
        t2 -->|"NON"| stop[Stop]
    end

    branch --> c1
```

### Workflow Auto-trigger — Schedule BigQuery

```mermaid
flowchart LR
    cron["⏰ Lun / Jeu / Sam\n8h00"] --> check

    subgraph check ["Vérification BigQuery"]
        q[__TABLES__\nlast_modified_time ≥ 72h] --> result{Changement ?}
    end

    fivetran[Fivetran] -->|Sync| bq[(linki_bucket_set\ngoogle_drive)]
    bq --> q

    result -->|❌ NON| stop[Workflow terminé\nRien à faire]
    result -->|✅ OUI| build[dbt build\n--target prod]
    build --> prod[(BigQuery\nbronze/silver/gold_linki)]
```

### Secrets requis

| Secret | Description |
|---|---|
| `GCP_SERVICE_ACCOUNT_KEY` | Contenu JSON du service account GCP |
| `GH_PAT` | Personal Access Token GitHub (scope `repo`) — utilisé pour le merge automatique des PRs |

### Documentation

La documentation dbt est générée automatiquement et publiée sur GitHub Pages à chaque merge dans `main`.

---

## Bonnes pratiques

- **Clés surrogates** : utilisation de `FARM_FINGERPRINT(CONCAT(...))` sur toutes les dimensions et facts
- **Déduplication** : `QUALIFY ROW_NUMBER() OVER (...) = 1` en couche intermediate
- **Modèles incrémentaux** : `fct_posts` et `fct_abonnes` en `merge` pour éviter les rechargements complets
- **Séparation dev/prod** : la macro `generate_schema_name` préfixe `dev_` en local, sans préfixe en prod
- **Tests systématiques** : chaque modèle a au minimum des tests `not_null` et `unique` sur ses clés
- **Packages externes cachés** : `dbt_utils` et `dbt_expectations` filtrés de la documentation publiée

---

## Limites & évolutions futures

### Limites actuelles

- **CI et dev sur les mêmes schemas** : le CI utilise le target `dev`, ce qui peut causer des conflits si un build local tourne en même temps
- **Pas de target staging** : les données prod ne sont pas validées sur un environnement miroir avant déploiement
- **dim_calendar hardcodée** : plage fixe 2021-2027, à mettre à jour manuellement
- **Pas de snapshots** : les changements historiques sur les dimensions ne sont pas trackés

### Évolutions recommandées

- **Ajouter un target `staging`** dans `auto-trigger.yml` : `dbt build staging → si OK → dbt build prod` pour protéger la prod des données mal formatées
- **Ajouter un target `ci` dédié** pour isoler complètement les builds CI des builds locaux
- **Notifications Slack** en cas d'échec CI/CD lorsqu'il s'agit d'une équipe
- **Snapshots** sur `dim_connections` pour tracker les évolutions dans le temps
- **Tests SQL custom** dans `/tests` pour les règles métier complexes

---

## Power BI

Le rapport Power BI consomme les tables de la couche Gold (`gold_linki`) pour visualiser les KPIs LinkedIn.

**KPIs suivis :**
- Évolution des impressions par post
- Évolution des abonnements dans le temps
- Performance des interactions (likes, commentaires, partages)
- Analyse démographique des abonnés (zones géographiques, secteurs, niveaux hiérarchiques)

![Dashboard Power BI](assets/impressions.png)
