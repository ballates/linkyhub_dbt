/*
  Modèle      : int_connections
  Source      : prime-force-478609-s4.bronze_linki.stg_connections
  Cible       : prime-force-478609-s4.silver_linki.int_connections

  Description :
    Déduplication par id_connection : on conserve la connexion la plus récente.
    Filtre sur les lignes sans prénom appliqué dans la couche mart.
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
    connected_on,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _at_load
FROM {{ ref('stg_connections') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_connection ORDER BY connected_on DESC NULLS LAST) = 1
