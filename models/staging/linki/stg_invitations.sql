/*
  Modèle      : stg_invitations
  Source      : prime-force-478609-s4.linki_bucket_set.invitations
  Cible       : prime-force-478609-s4.bronze_linki.stg_invitations

  Description :
    Renommage et typage des colonnes des invitations LinkedIn issues du bucket GCS via Fivetran.
    Génération de la surrogate key sur l'expéditeur, le destinataire et la date d'envoi.
*/

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['`From`', '`To`', '`Sent At`']) }} AS id_invitation,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    `From`              AS sender,
    `To`                AS recipient,
    `Sent At`           AS sent_at,
    Message             AS message,
    Direction           AS direction,
    inviterProfileUrl   AS inviter_profile_url,
    inviteeProfileUrl   AS invitee_profile_url,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ source('linki_bucket_set', 'invitations') }}
