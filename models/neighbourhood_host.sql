CREATE VIEW neighbourhoodDm AS
WITH base AS (
  SELECT
    host_neighbourhood_lga,
    DATE_TRUNC('month', scraped_date) AS month_year,
    COUNT(*) FILTER (WHERE has_availability = 't') AS active_listings,
    COUNT(*) AS total_listings,
    COUNT(DISTINCT host_id) FILTER (WHERE host_is_superhost = 't') AS superhost_count,
    COUNT(DISTINCT host_id) AS total_host_count,
    SUM(30 - availability_30) FILTER (WHERE has_availability = 't') AS number_of_stays,
    SUM((30 - availability_30) * price) FILTER (WHERE has_availability = 't') AS estimated_revenue
  FROM listings_all
  LEFT JOIN neighbourhood_to_lga_mapping ON neighbourhood_to_lga_mapping.host_neighbourhood = listings_all.host_neighbourhood
  GROUP BY host_neighbourhood_lga, month_year
),
metrics AS (
  SELECT
    host_neighbourhood_lga,
    month_year,
    active_listings,
    total_listings,
    superhost_count,
    total_host_count,
    number_of_stays,
    estimated_revenue,
    (active_listings::FLOAT / total_listings) * 100 AS active_listing_rate,
    (superhost_count::FLOAT / total_host_count) * 100 AS superhost_rate,
    estimated_revenue / NULLIF(total_host_count, 0) AS estimated_revenue_per_host
  FROM base
)
SELECT
  host_neighbourhood_lga,
  month_year,
  active_listing_rate,
  superhost_rate,
  number_of_stays,
  estimated_revenue,
  estimated_revenue_per_host,
  LAG(estimated_revenue, 1) OVER (PARTITION BY host_neighbourhood_lga ORDER BY month_year) AS previous_month_revenue,
  (estimated_revenue - LAG(estimated_revenue, 1) OVER (PARTITION BY host_neighbourhood_lga ORDER BY month_year)) / NULLIF(LAG(estimated_revenue, 1) OVER (PARTITION BY host_neighbourhood_lga ORDER BY month_year), 0) * 100 AS percentage_change
FROM metrics
ORDER BY host_neighbourhood_lga, month_year;
