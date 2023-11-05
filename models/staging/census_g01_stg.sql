{{ config(materialized='view') }}

WITH transformed_data AS (
  SELECT
    LGA_CODE_2016,
    CAST(REPLACE(Tot_P_M, ',', '') AS INT) AS total_persons_male,
    CAST(REPLACE(Tot_P_F, ',', '') AS INT) AS total_persons_female,
    CAST(REPLACE(Tot_P_P, ',', '') AS INT) AS total_persons,
    
    -- Transformations for age ranges
    CAST(REPLACE(Age_5_14_yr_M, ',', '') AS INT) AS age_5_14_years_male,
    CAST(REPLACE(Age_5_14_yr_F, ',', '') AS INT) AS age_5_14_years_female,
    CAST(REPLACE(Age_5_14_yr_P, ',', '') AS INT) AS age_5_14_years_total,
    
    CAST(REPLACE(Age_15_19_yr_M, ',', '') AS INT) AS age_15_19_years_male,
    CAST(REPLACE(Age_15_19_yr_F, ',', '') AS INT) AS age_15_19_years_female,
    CAST(REPLACE(Age_15_19_yr_P, ',', '') AS INT) AS age_15_19_years_total,    
    CASE WHEN total_persons > 0 THEN CAST(age_0_4_years_total AS FLOAT) / total_persons ELSE 0 END AS percentage_age_0_4,
    CASE WHEN total_persons > 0 THEN CAST(age_5_14_years_total AS FLOAT) / total_persons ELSE 0 END AS percentage_age_5_14,
    CASE WHEN total_persons > 0 THEN CAST(age_15_19_years_total AS FLOAT) / total_persons ELSE 0 END AS percentage_age_15_19,
    

  FROM {{ source('raw', 'census_g01_nsw_lga_model') }}
)

SELECT *
FROM transformed_data;
