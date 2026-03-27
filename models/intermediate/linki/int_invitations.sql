SELECT
    id_invitation,
    sender,
    recipient,
    sent_at,
    message,
    direction,
    inviter_profile_url,
    invitee_profile_url,
    _at_load
FROM {{ ref('stg_invitations') }}