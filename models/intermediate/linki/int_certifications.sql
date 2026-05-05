/*
  Modèle      : int_certifications
  Source      : prime-force-478609-s4.bronze_linki.stg_certifications
  Cible       : prime-force-478609-s4.silver_linki.int_certifications

  Description :
    Passage direct depuis le staging. Aucune transformation supplémentaire.
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
    license_number,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _at_load
FROM {{ ref('stg_certifications') }}
