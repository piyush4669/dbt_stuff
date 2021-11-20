WITH source_covid_data AS (
    select * from {{ source('covid_data', 'COVID_19_INDONESIA_PIYUSH_AGARWAL') }}
),

final AS (
    SELECT CAST(source_covid_data.date AS DATE) AS temp_date, new_cases, new_deaths, new_recovered
    FROM source_covid_data
),

getmonthyear AS (
    SELECT MONTH(temp_date) AS Month_, YEAR(temp_date) AS Year_, CONCAT(CONCAT(Month_,'-'),Year_) AS year_month, SUM(new_cases) AS new_cases_month, SUM(new_deaths) AS new_death_month, SUM(new_recovered) AS new_recovered_month
    FROM final
    GROUP BY Month_,Year_
    ORDER BY Year_, Month_
)

SELECT * FROM getmonthyear