WITH source_covid_data AS (
    select * from {{ source('covid_data', 'COVID_19_INDONESIA_PIYUSH_AGARWAL') }}
),

final AS (
    SELECT CAST(source_covid_data.date AS DATE) AS temp_date, total_cases, population
    FROM source_covid_data
),

population_infected AS (
    SELECT MONTH(temp_date) AS Month_, YEAR(temp_date) AS Year_, CONCAT(CONCAT(Month_,'-'),Year_) AS year_month, ((max(total_cases)/max(population))*100) AS Percent_Population_Infected
    FROM final
    GROUP BY Month_,Year_
    ORDER BY Year_, Month_
)

SELECT * FROM population_infected