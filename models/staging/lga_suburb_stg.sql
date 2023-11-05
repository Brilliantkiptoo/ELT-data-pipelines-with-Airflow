{{ config(materialized='view') }}

-- CTE to clean and standardize the LGA and suburb names
WITH clean_data AS (
  SELECT
    -- Remove any leading/trailing whitespace and convert to uppercase for consistency
    TRIM(UPPER(LGA_NAME)) as lga_name,
    TRIM(UPPER(SUBURB_NAME)) as suburb_name
  FROM {{ source('raw', 'nsw_lga_suburb_model') }}
)

-- Select all cleaned data from the CTE
SELECT *
FROM clean_data;
