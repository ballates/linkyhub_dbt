/*
  Modèle      : int_invitations
  Source      : prime-force-478609-s4.bronze_linki.stg_invitations
  Cible       : prime-force-478609-s4.silver_linki.int_invitations

  Description :
    Passage direct depuis le staging. Aucune transformation supplémentaire.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    id_invitation,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    sender,
    recipient,
    sent_at,
    message,
    direction,
    inviter_profile_url,
    invitee_profile_url,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    _at_load
FROM {{ ref('stg_invitations') }}
