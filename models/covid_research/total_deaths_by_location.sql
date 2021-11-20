WITH source_covid_data AS (
    select * from {{ source('covid_data', 'COVID_19_INDONESIA_PIYUSH_AGARWAL') }}
),

final AS (
    SELECT Province, MAX(TOTAL_DEATHS) AS total_deaths
    FROM source_covid_data
    WHERE Province IS NOT NULL
    GROUP BY Province
)

SELECT * FROM final