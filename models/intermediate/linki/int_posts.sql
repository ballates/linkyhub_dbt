/*
  Modèle      : int_posts
  Source      : prime-force-478609-s4.bronze_linki.stg_posts
  Cible       : prime-force-478609-s4.silver_linki.int_posts

  Description :
    Extraction du titre du post depuis le slug de l'URL (nouveau format LinkedIn uniquement).

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  ESTIMATION DES IMPRESSIONS PAR PRO-RATA TEMPORIS                          │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │  Problème :                                                                 │
  │    LinkedIn exporte les impressions sur une fenêtre GLISSANTE d'un an.     │
  │    Chaque export recouvre partiellement le précédent → double comptage     │
  │    si on fait une simple somme.                                             │
  │                                                                             │
  │  Exemple :                                                                  │
  │    Export 1 (fin Jan 2025) : Jan 2024 → Jan 2025  = 1200 impressions       │
  │    Export 2 (fin Avr 2025) : Avr 2024 → Avr 2025  =   45 impressions      │
  │    → Chevauchement : Avr 2024 → Jan 2025 (9 mois)                          │
  │    → Partie nouvelle de Export 2 : Jan 2025 → Avr 2025 = 85 jours         │
  │                                                                             │
  │  Solution : Pro-rata temporis                                               │
  │    Pour chaque export, on ne retient que la fraction correspondant          │
  │    aux jours NON couverts par l'export précédent :                         │
  │                                                                             │
  │      contribution = impressions × (jours_nouveaux / 365)                   │
  │                                                                             │
  │    Le premier export (sans précédent) compte pour 100%.                    │
  │    On somme ensuite toutes les contributions pour obtenir                   │
  │    le total estimé d'impressions sur toute la vie du post.                 │
  │                                                                             │
  │  Exemple chiffré :                                                          │
  │    Export 1 : 1200 × (365/365) = 1200                                      │
  │    Export 2 :   45 × ( 85/365) =   10                                      │
  │    Export 3 : 1000 × (183/365) =  501                                      │
  │    Export 4 :  500 × ( 92/365) =  126                                      │
  │                               ──────                                        │
  │    Total estimé               = 1837 impressions                            │
  │                                                                             │
  │  Hypothèse : distribution uniforme des impressions sur l'année.            │
  │  Limite : estimation approximative — les données journalières               │
  │  LinkedIn ne sont pas exportables.                                          │
  └─────────────────────────────────────────────────────────────────────────────┘
*/

WITH base AS (
    SELECT
        id_post,
        {{ decode_url_slug(
            "REGEXP_REPLACE(REPLACE(REGEXP_EXTRACT(url_du_post, r'_(.+)-\\d{10,}'), '-', ' '), r'\\s*(ugcPost|share)\\s*$', '')"
        ) }}                                                                 AS title_post,
        impressions,
        url_du_post,
        date_de_publication_du_post,
        SAFE.PARSE_DATE('%Y_%m_%d', RIGHT(_periode, 10))                     AS fin_periode,
        _at_load
    FROM {{ ref('stg_posts') }}
),

pro_rata AS (
    SELECT
        id_post,
        title_post,
        url_du_post,
        date_de_publication_du_post,
        fin_periode,
        impressions,
        LAG(fin_periode) OVER (
            PARTITION BY id_post
            ORDER BY fin_periode
        )                                                                    AS fin_periode_precedente,
        _at_load
    FROM base
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_post,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    MAX(title_post)                                                          AS title_post,
    CAST(ROUND(SUM(
        CASE
            WHEN fin_periode_precedente IS NULL
                THEN impressions
            ELSE impressions * DATE_DIFF(fin_periode, fin_periode_precedente, DAY) / 365
        END
    )) AS INT64)                                                             AS impressions,
    MAX(url_du_post)                                                         AS url_post,
    SAFE.PARSE_DATE('%d/%m/%Y', MAX(date_de_publication_du_post))            AS date_publication_post,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    MAX(_at_load)                                                            AS _at_load
FROM pro_rata
GROUP BY id_post
