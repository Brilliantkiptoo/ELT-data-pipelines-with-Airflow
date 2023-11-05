{{ config(materialized='view') }}

WITH transformed_data AS (
  SELECT
    LGA_CODE_2016,
    CAST(Median_age_persons AS INT) AS median_age_persons,
    CAST(REPLACE(Median_mortgage_repay_monthly, '$', '') AS INT) AS median_mortgage_repayment_monthly,
    CAST(REPLACE(Median_tot_prsnl_inc_weekly, '$', '') AS INT) AS median_total_personal_income_weekly,
    CAST(REPLACE(Median_rent_weekly, '$', '') AS INT) AS median_rent_weekly,
    CAST(REPLACE(Median_tot_fam_inc_weekly, '$', '') AS INT) AS median_total_family_income_weekly,
    CAST(Average_num_psns_per_bedroom AS FLOAT) AS average_number_persons_per_bedroom,
    CAST(REPLACE(Median_tot_hhd_inc_weekly, '$', '') AS INT) AS median_total_household_income_weekly,
    CAST(Average_household_size AS FLOAT) AS average_household_size,
    
    --Ratio of income to rent
    CASE WHEN median_total_personal_income_weekly > 0 THEN median_rent_weekly::FLOAT / median_total_personal_income_weekly ELSE NULL END AS rent_to_income_ratio,
    
    -- Ratio of income to mortgage repayments
    CASE WHEN median_total_personal_income_weekly > 0 THEN median_mortgage_repayment_monthly::FLOAT / (median_total_personal_income_weekly * 4) ELSE NULL END AS mortgage_to_income_ratio

  FROM {{ source('raw', 'census_g02_nsw_lga_model') }}
)

SELECT *
FROM transformed_data;
