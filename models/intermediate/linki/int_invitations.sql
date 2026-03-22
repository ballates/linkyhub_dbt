SELECT
    FARM_FINGERPRINT(CONCAT(`From`, COALESCE(`To`, ''), COALESCE(`Sent At`, ''))) AS id_invitation,
    `From`              AS sender,
    `To`                AS recipient,
    `Sent At`           AS sent_at,
    Message             AS message,
    Direction           AS direction,
    inviterProfileUrl   AS inviter_profile_url,
    inviteeProfileUrl   AS invitee_profile_url,
    CURRENT_TIMESTAMP() AS _at_load
FROM {{ ref('stg_invitations') }}