SELECT
    FARM_FINGERPRINT(CONCAT(first_name, last_name, COALESCE(url, ''))) AS connection_id,
    first_name,
    last_name,
    url,
    email_address,
    company,
    position,
    connected_on
FROM {{ ref('int_connections') }}
WHERE first_name IS NOT NULL