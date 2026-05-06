/*
  Modèle      : fct_posts
  Source      : prime-force-478609-s4.silver_linki.int_posts
                prime-force-478609-s4.silver_linki.int_interactions
                prime-force-478609-s4.gold_linki.fct_engagement
  Cible       : prime-force-478609-s4.gold_linki.fct_posts

  Description :
    Table de faits des posts LinkedIn. Joint les impressions (int_posts), les interactions
    (int_interactions) et l'engagement (fct_engagement, is_own_post = TRUE) par id_post.
    interactions peut être NULL si le post est absent de l'export LinkedIn (post trop ancien).
    nb_comments + nb_reactions = mon activité sur mes propres posts (is_own_post = TRUE).
    LIMITE : ne capture pas les comments/réactions des autres membres sous ses posts.
    interactions - (nb_comments + nb_reactions) ≈ activité audience (comments + réactions + republications des autres).

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │  STRATÉGIE INCRÉMENTALE                                                     │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │  Matérialisation : incremental, stratégie merge sur id_post.                │
  │                                                                             │
  │  Premier run (full refresh) :                                               │
  │    Charge l'intégralité des posts depuis int_posts et int_interactions.     │
  │                                                                             │
  │  Runs suivants (incrémental) :                                              │
  │    Retraite l'intégralité des posts à chaque run (pas de filtre WHERE).    │
  │    Le merge sur id_post gère les inserts ET les updates :                  │
  │      - Nouveau post → INSERT                                                │
  │      - Post existant avec impressions ou interactions révisées → UPDATE    │
  │                                                                             │
  │    Pourquoi pas de filtre WHERE ?                                           │
  │    Un filtre sur date_publication_post bloquerait la mise à jour des       │
  │    impressions (recalculées pro-rata à chaque export) et des interactions  │
  │    (qui peuvent évoluer sur d'anciens posts). Le dataset étant petit       │
  │    (LinkedIn personnel), le full scan est négligeable.                     │
  └─────────────────────────────────────────────────────────────────────────────┘
*/

{{ config(
    materialized='incremental',
    unique_key=['id_post'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH posts AS (
    SELECT
        id_post,
        url_post,
        date_publication_post,
        impressions
    FROM {{ ref('int_posts') }}
),

interactions AS (
    SELECT
        id_post,
        interactions
    FROM {{ ref('int_interactions') }}
),

engagement AS (
    SELECT
        post_id,
        COUNTIF(type_event = 'comment')  AS nb_comments,
        COUNTIF(type_event = 'reaction') AS nb_reactions
    FROM {{ ref('fct_engagement') }}
    WHERE is_own_post = TRUE
    GROUP BY post_id
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    p.id_post,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    p.url_post,
    p.date_publication_post,
    p.impressions,
    i.interactions,
    COALESCE(e.nb_comments, 0)      AS nb_comments,
    COALESCE(e.nb_reactions, 0)     AS nb_reactions,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP()             AS _at_load
FROM posts p
LEFT JOIN interactions i
    ON p.id_post = i.id_post
LEFT JOIN engagement e
    ON p.id_post = e.post_id
