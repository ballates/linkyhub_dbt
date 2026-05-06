/*
  Modèle      : stg_invitations
  Source      : prime-force-478609-s4.linky_bucket_landing.invitations
  Cible       : prime-force-478609-s4.bronze_linki.stg_invitations

  Description :
    Renommage et typage des colonnes des invitations LinkedIn issues du bucket GCS via Fivetran.
    Données chargées via Fivetran depuis GCS (linki_bucket).
    Génération de la surrogate key sur l'expéditeur, le destinataire et la date d'envoi.
    Note : `from` et `to` sont des mots réservés SQL, aliasés en sender/recipient.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['`from`', '`to`', 'sent_at']) }} AS id_invitation,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    `from`               AS sender,
    `to`                 AS recipient,
    sent_at,
    direction,
    inviter_profile_url,
    invitee_profile_url,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ source('linky_bucket_landing', 'invitations') }}
