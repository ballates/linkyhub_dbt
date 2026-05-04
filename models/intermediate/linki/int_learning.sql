/*
  Modèle      : int_learning
  Source      : prime-force-478609-s4.bronze_linki.stg_learning
  Cible       : prime-force-478609-s4.silver_linki.int_learning

  Description :
    Parsing des timestamps de consultation et de complétion.
    Déduplication par titre : on conserve la consultation la plus récente.
    Filtre sur les lignes sans titre.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['title']) }}               AS id_learning,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    title,
    description,
    type,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M UTC', last_watched_date)   AS last_watched_date,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M UTC', completed_at)        AS completed_at,
    saved,
    notes,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _at_load
FROM {{ ref('stg_learning') }}
WHERE title IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY title ORDER BY last_watched_date DESC NULLS LAST) = 1
