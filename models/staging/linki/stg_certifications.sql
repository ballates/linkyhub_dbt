SELECT
    FARM_FINGERPRINT(CONCAT(
        COALESCE(Name, ''), '|',
        COALESCE(Authority, ''), '|',
        COALESCE(`License Number`, '')
    ))                                           AS id_certification,
    Name                                         AS name,
    Url                                          AS url,
    Authority                                    AS authority,
    SAFE.PARSE_TIMESTAMP('%b %Y', `Started On`)  AS started_on,
    SAFE.PARSE_TIMESTAMP('%b %Y', `Finished On`) AS finished_on,
    `License Number`                             AS license_number,
    CURRENT_TIMESTAMP()                          AS _at_load
FROM {{ source('linki_bucket_set', 'certifications') }}