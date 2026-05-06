/*
  Modèle      : stg_connections
  Source      : prime-force-478609-s4.linki_bucket_set.connections
  Cible       : prime-force-478609-s4.bronze_linki.stg_connections

  Description :
    Renommage, typage et parsing multi-format des dates de connexion LinkedIn (formats FR et EN).
    Normalisation de l'adresse email. Génération de la surrogate key sur prénom, nom, URL et email.
*/

WITH renamed AS (
    SELECT
        string_field_0  AS first_name,
        string_field_1  AS last_name,
        string_field_2  AS url,
        string_field_3  AS email_address,
        string_field_4  AS company,
        string_field_5  AS position,
        string_field_6  AS connected_on
    FROM {{ source('linki_bucket_set', 'Connections') }}
)

SELECT
    -- ================================================================
    -- CLÉ TECHNIQUE / SURROGATE KEY
    -- ================================================================
    {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name', 'url', 'email_address']) }} AS id_connection,

    -- ================================================================
    -- COLONNES MÉTIER
    -- ================================================================
    first_name,
    last_name,
    url,
    {{ normalize_email('email_address') }}  AS email_address,
    company,
    position,
    COALESCE(
        -- Format français : "11-janv-26", "27-nov-25"
        SAFE.PARSE_DATE('%d-%b-%y',
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            LOWER(connected_on),
            'janv','jan'),'févr','feb'),'mars','mar'),'avr','apr'),
            'mai','may'),'juin','jun'),'juil','jul'),'août','aug'),
            'sept','sep'),'oct','oct'),'nov','nov'),'déc','dec')
        ),
        -- Format anglais court : "27 Nov 25"
        SAFE.PARSE_DATE('%d %b %y', connected_on),
        -- Format anglais long : "31 Dec 2025"
        SAFE.PARSE_DATE('%d %b %Y', connected_on)
    )                           AS connected_on,

    -- ================================================================
    -- MÉTADONNÉES DE TRAÇABILITÉ
    -- ================================================================
    CURRENT_TIMESTAMP()         AS _at_load
FROM renamed
