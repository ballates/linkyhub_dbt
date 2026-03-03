SELECT
    FARM_FINGERPRINT(CONCAT(sender, COALESCE(recipient, ''), COALESCE(sent_at, ''))) AS invitation_ID,
    sender,
    recipient,
    direction,
    sent_at,
    message,
    inviter_profile_url,
    invitee_profile_url
FROM {{ ref('int_invitations') }}