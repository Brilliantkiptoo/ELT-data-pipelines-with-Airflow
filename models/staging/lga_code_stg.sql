{{ config(materialized='view') }}

WITH standardized_data AS (
  SELECT
    -- Ensure LGA_CODE is an integer, and handle potential casting errors or nulls
    COALESCE(NULLIF(TRIM(LGA_CODE), ''), '0')::INT as lga_code,
    
    -- replace any known incorrect names with the correct ones
    CASE
      WHEN TRIM(UPPER(LGA_NAME)) = 'INCORRECT_NAME' THEN 'CORRECT_NAME'
      ELSE TRIM(UPPER(LGA_NAME))
    END as lga_name
  FROM {{ source('raw', 'nsw_lga_code_model') }}
)

-- Select all standardized data from the CTE
SELECT *
FROM standardized_data;
