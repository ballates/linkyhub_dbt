SELECT
    id_connection,
    first_name,
    last_name,
    url,
    email_address,
    company,
    position,
    connected_on,
    _at_load
FROM {{ ref('stg_connections') }}