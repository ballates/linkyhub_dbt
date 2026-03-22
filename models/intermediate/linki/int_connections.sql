SELECT
    FARM_FINGERPRINT(CONCAT(
        COALESCE(`First Name`, 'NO_FNAME'), '|',
        COALESCE(`Last Name`, 'NO_LNAME'), '|',
        COALESCE(URL, 'NO_URL'), '|',
        COALESCE(`Email Address`, 'NO_EMAIL')
    )) AS id_connection,
    `First Name`                AS first_name,
    `Last Name`                 AS last_name,
    URL                         AS url,
    {{ normalize_email('`Email Address`') }} AS email_address,
    Company                     AS company,
    Position                    AS position,
    COALESCE(
        -- Format français : "11-janv-26", "27-nov-25"
        SAFE.PARSE_DATE('%d-%b-%y',
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            LOWER(`Connected On`),
            'janv','jan'),'févr','feb'),'mars','mar'),'avr','apr'),
            'mai','may'),'juin','jun'),'juil','jul'),'août','aug'),
            'sept','sep'),'oct','oct'),'nov','nov'),'déc','dec')
        ),
        -- Format anglais court : "27 Nov 25"
        SAFE.PARSE_DATE('%d %b %y', `Connected On`),
        -- Format anglais long : "31 Dec 2025"
        SAFE.PARSE_DATE('%d %b %Y', `Connected On`)
    )                           AS connected_on
FROM {{ ref('stg_connections') }}