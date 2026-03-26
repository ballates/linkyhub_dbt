WITH date_spine AS (
    {{ generate_date_spine('2021-01-01', '2027-12-31') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS id_calendar,
    date,
    EXTRACT(YEAR FROM date)                 AS annee,
    EXTRACT(QUARTER FROM date)              AS trimestre,
    EXTRACT(MONTH FROM date)                AS mois,
    FORMAT_DATE('%B', date)                 AS nom_mois,
    FORMAT_DATE('%b', date)                 AS nom_mois_court,
    EXTRACT(WEEK FROM date)                 AS semaine,
    EXTRACT(DAY FROM date)                  AS jour,
    FORMAT_DATE('%A', date)                 AS nom_jour,
    IF(EXTRACT(DAYOFWEEK FROM date) IN (1, 7), TRUE, FALSE) AS est_weekend
FROM date_spine