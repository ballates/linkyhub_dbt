/*
  Modèle      : stg_connections
  Source      : prime-force-478609-s4.linky_bucket_landing.connections
  Cible       : prime-force-478609-s4.bronze_linki.stg_connections

  Description :
    Typage et parsing multi-format des dates de connexion LinkedIn (formats FR et EN).
    Données chargées via Fivetran depuis GCS (linki_bucket).
    Normalisation de l'adresse email. Génération de la surrogate key sur prénom, nom, URL et email.
*/

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
FROM {{ source('linky_bucket_landing', 'connections') }}
