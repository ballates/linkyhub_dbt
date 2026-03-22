SELECT
    id_invitation,
    sender,
    recipient,
    direction,
    sent_at,
    message,
    inviter_profile_url,
    invitee_profile_url
FROM {{ ref('int_invitations') }}