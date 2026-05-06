/*
  Modèle      : dim_invitations
  Source      : prime-force-478609-s4.silver_linki.int_invitations
  Cible       : prime-force-478609-s4.gold_linki.dim_invitations

  Description :
    Dimension des invitations LinkedIn envoyées et reçues.
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
    direction,
    sent_at,
    inviter_profile_url,
    invitee_profile_url
FROM {{ ref('int_invitations') }}
