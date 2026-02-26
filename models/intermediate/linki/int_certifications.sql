SELECT
    Name                                        AS name,
    Url                                         AS url,
    Authority                                   AS authority,
    SAFE.PARSE_TIMESTAMP('%b %Y', `Started On`) AS started_on,
    SAFE.PARSE_TIMESTAMP('%b %Y', `Finished On`) AS finished_on,
    `License Number`                            AS license_number

FROM {{ ref('stg_certifications') }}