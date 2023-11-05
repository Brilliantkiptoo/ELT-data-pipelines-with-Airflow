{{ config(materialized='view') }}

WITH cleaned_data AS (
  SELECT
    -- Cast IDs and ensure they are integers
    CAST(LISTING_ID as INT) as listing_id,
    CAST(SCRAPE_ID as BIGINT) as scrape_id,
    
    -- Convert dates to the proper format
    TO_DATE(NULLIF(SCRAPED_DATE, ''), 'MM/DD/YYYY') as scraped_date,
    
    -- Ensure host information is properly formatted and handle missing values
    HOST_ID,
    HOST_NAME,
    TO_DATE(NULLIF(HOST_SINCE, ''), 'DD/MM/YYYY') as host_since,
    
    -- Convert boolean flags to true/false
    CASE WHEN HOST_IS_SUPERHOST = 't' THEN TRUE ELSE FALSE END as host_is_superhost,
    
    -- Standardize and clean neighborhood names
    UPPER(TRIM(HOST_NEIGHBOURHOOD)) as host_neighbourhood,
    UPPER(TRIM(LISTING_NEIGHBOURHOOD)) as listing_neighbourhood,
    
    -- Additional property details
    UPPER(TRIM(PROPERTY_TYPE)) as property_type,
    UPPER(TRIM(ROOM_TYPE)) as room_type,
    CAST(ACCOMMODATES as INT) as accommodates,
    
    -- Clean and convert price to a numeric format, removing any currency symbols or commas
    CAST(REPLACE(REPLACE(PRICE, '$', ''), ',', '') as NUMERIC) as price,
    
    -- Convert availability flag to boolean
    CASE WHEN HAS_AVAILABILITY = 't' THEN TRUE ELSE FALSE END as has_availability,
    
    -- Ensure counts are integers
    CAST(AVAILABILITY_30 as INT) as availability_30,
    CAST(NUMBER_OF_REVIEWS as INT) as number_of_reviews,
    
    -- Convert review scores to integers, handling missing values
    CAST(NULLIF(REVIEW_SCORES_RATING, '') as INT) as review_scores_rating,
    CAST(NULLIF(REVIEW_SCORES_ACCURACY, '') as INT) as review_scores_accuracy,
    -- ... and so on for the rest of the review scores columns ...
    CAST(NULLIF(REVIEW_SCORES_VALUE, '') as INT) as review_scores_value
  
    
  FROM {{ source('raw', 'all_month_lsiting_model') }}
  WHERE LISTING_ID IS NOT NULL AND SCRAPE_ID IS NOT NULL
)
SELECT *
FROM cleaned_data;
