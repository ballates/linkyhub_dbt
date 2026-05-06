/*
  Modèle      : fct_engagement
  Source      : prime-force-478609-s4.silver_linki.int_engagement
  Cible       : prime-force-478609-s4.gold_linki.fct_engagement

  Description :
    Fait d'engagement LinkedIn : commentaires et réactions réalisés depuis mon compte.
    Couvre aussi bien mon activité sur mes propres posts que sur les posts d'autres membres.
    is_own_post = TRUE  → engagement sur un de mes posts (présent dans dim_posts)
    is_own_post = FALSE → engagement sur le post d'un autre membre LinkedIn
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    e.id_engagement,

    -- ================================================================
    -- CLÉ ÉTRANGÈRE
    -- ================================================================
    e.post_id,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    e.type_event,
    e.date,
    e.link,
    e.content,
    p.id_post IS NOT NULL   AS is_own_post
FROM {{ ref('int_engagement') }} e
LEFT JOIN {{ ref('dim_posts') }} p
    ON e.post_id = p.id_post