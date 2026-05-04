/*
  Modèle      : dim_learning
  Source      : prime-force-478609-s4.silver_linki.int_learning
  Cible       : prime-force-478609-s4.gold_linki.dim_learning

  Description :
    Dimension des formations LinkedIn Learning consultées ou complétées.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_learning,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    title,
    description,
    type,
    last_watched_date,
    completed_at,
    saved,
    notes
FROM {{ ref('int_learning') }}
WHERE title IS NOT NULL
