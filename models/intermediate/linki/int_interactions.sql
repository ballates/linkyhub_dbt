/*
  Modèle      : int_interactions
  Source      : prime-force-478609-s4.bronze_linki.stg_interactions
  Cible       : prime-force-478609-s4.silver_linki.int_interactions

  Description :
    Extraction du titre du post depuis le slug de l'URL (nouveau format LinkedIn uniquement).
    Déduplication par id_post : on conserve uniquement l'export le plus récent (_periode DESC).

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  POURQUOI PAS DE PRO-RATA POUR LES INTERACTIONS ?                          │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │  Les interactions (likes, commentaires, partages) sont une métrique de      │
  │  STOCK : chaque export donne le total cumulé actuel du post à l'instant T. │
  │  Il ne s'agit pas d'une fenêtre glissante comme pour les impressions.      │
  │                                                                             │
  │  De plus, les interactions peuvent BAISSER entre deux exports               │
  │  (ex : un utilisateur retire son like).                                     │
  │                                                                             │
  │  Exemple :                                                                  │
  │    Export Jan 2025 : post A = 150 interactions                              │
  │    Export Avr 2025 : post A = 143 interactions  (unlike entre-temps)        │
  │                                                                             │
  │  → Un pro-rata donnerait 150×(365/365) + 143×(85/365) = 183  — absurde.   │
  │  → La vraie valeur est 143 : le dernier export fait foi.                    │
  │                                                                             │
  │  Stratégie retenue : conserver uniquement l'export le plus récent           │
  │  (QUALIFY ROW_NUMBER() PARTITION BY id_post ORDER BY fin_periode DESC).     │
  └─────────────────────────────────────────────────────────────────────────────┘
*/

WITH base AS (
    SELECT
        {{ decode_url_slug(
            "REGEXP_REPLACE(REPLACE(REGEXP_EXTRACT(url_du_post, r'_(.+)-\\d{10,}'), '-', ' '), r'\\s*(ugcPost|share)\\s*$', '')"
        ) }}                                                                 AS title_post,
        id_post,
        url_du_post,
        date_de_publication_du_post,
        interactions,
        _at_load
    FROM {{ ref('stg_interactions') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_post
        ORDER BY SAFE.PARSE_DATE('%Y_%m_%d', RIGHT(_periode, 10)) DESC
    ) = 1
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_post,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    title_post,
    url_du_post                                                              AS url_post,
    SAFE.PARSE_DATE('%d/%m/%Y', date_de_publication_du_post)                 AS date_publication_post,
    interactions,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _at_load
FROM base
