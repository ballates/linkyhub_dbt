/*
  Modèle      : dim_connections
  Source      : prime-force-478609-s4.silver_linki.int_connections
  Cible       : prime-force-478609-s4.gold_linki.dim_connections

  Description :
    Dimension des connexions LinkedIn. Filtre sur les lignes sans prénom.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_connection,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    first_name,
    last_name,
    url,
    email_address,
    company,
    position,
    connected_on
FROM {{ ref('int_connections') }}
WHERE first_name IS NOT NULL
