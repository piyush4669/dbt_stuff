WITH source_covid_data AS (
    select * from {{ source('covid_data', 'COVID_19_INDONESIA_PIYUSH_AGARWAL') }}
),

final AS (
    SELECT CAST(source_covid_data.date AS DATE) AS temp_date, new_cases, new_deaths, new_recovered
    FROM source_covid_data
),

getmonthyear AS (
    SELECT CONCAT(CONCAT(MONTH(temp_date),'/'),YEAR(temp_date)) AS year_month, SUM(new_cases) AS new_cases_month, SUM(new_deaths) AS new_death_month, SUM(new_recovered) AS new_recovered_month
    FROM final
    GROUP BY year_month
    ORDER BY year_month
)

SELECT * FROM getmonthyear