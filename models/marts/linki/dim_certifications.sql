/*
  Modèle      : dim_certifications
  Source      : prime-force-478609-s4.silver_linki.int_certifications
  Cible       : prime-force-478609-s4.gold_linki.dim_certifications

  Description :
    Dimension des certifications LinkedIn obtenues.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_certification,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    name,
    url,
    authority,
    started_on,
    finished_on,
    license_number
FROM {{ ref('int_certifications') }}
