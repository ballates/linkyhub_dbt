/*
  Modèle      : stg_invitations
  Source      : prime-force-478609-s4.linki_bucket_set.invitations
  Cible       : prime-force-478609-s4.bronze_linki.stg_invitations

  Description :
    Renommage et typage des colonnes des invitations LinkedIn issues du bucket GCS via Fivetran.
    Génération de la surrogate key sur l'expéditeur, le destinataire et la date d'envoi.
*/

WITH renamed AS (
    SELECT
        string_field_0  AS sender,
        string_field_1  AS recipient,
        string_field_2  AS sent_at,
        string_field_3  AS message,
        string_field_4  AS direction,
        string_field_5  AS inviter_profile_url,
        string_field_6  AS invitee_profile_url
    FROM {{ source('linki_bucket_set', 'Invitations') }}
    WHERE string_field_0 != 'From'
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['sender', 'recipient', 'sent_at']) }} AS id_invitation,

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
    CURRENT_TIMESTAMP() AS _at_load
FROM renamed
