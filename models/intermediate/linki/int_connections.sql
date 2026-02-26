SELECT
    `First Name`                                            AS first_name,
    `Last Name`                                             AS last_name,
    URL                                                     AS url,
    `Email Address`                                         AS email_address,
    Company                                                 AS company,
    Position                                                AS position,
    SAFE.PARSE_TIMESTAMP('%d-%b-%y', `Connected On`)        AS connected_on,
    CURRENT_TIMESTAMP()                                     AS _at_load

FROM {{ ref('stg_connections') }}