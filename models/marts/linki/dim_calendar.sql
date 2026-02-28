WITH date_spine AS (
    SELECT date
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            DATE '2021-01-01',
            DATE '2026-12-31'
        )
    ) AS date
)

SELECT
    date,
    EXTRACT(YEAR FROM date)                 AS annee,
    EXTRACT(QUARTER FROM date)              AS trimestre,
    EXTRACT(MONTH FROM date)                AS mois,
    FORMAT_DATE('%B', date)                 AS nom_mois,
    EXTRACT(WEEK FROM date)                 AS semaine,
    EXTRACT(DAY FROM date)                  AS jour,
    FORMAT_DATE('%A', date)                 AS nom_jour,
    IF(EXTRACT(DAYOFWEEK FROM date) IN (1, 7), TRUE, FALSE) AS est_weekend
FROM date_spine