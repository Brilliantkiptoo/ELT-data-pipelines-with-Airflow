{{ config(materialized='table') }}

with listing_aggregates as (
    select
        l.listing_id,
        l.host_id,
        count(*) as total_listings,
        sum(case when l.has_availability = 't' then 1 else 0 end) as total_active_listings,
        avg(l.review_scores_rating) as average_rating
    from {{ ref('listings_all_stg') }} l
    group by l.listing_id, l.host_id
),

host_data as (
    select
        h.host_id,
        h.host_name,
        h.host_since
    from {{ ref('dim_host') }} h
),

fact_listings as (
    select
        a.listing_id,
        a.host_id,
        h.host_name,
        h.host_since,
        a.total_listings,
        a.total_active_listings,
        a.average_rating
    from listing_aggregates a
    left join host_data h on a.host_id = h.host_id
)

select
    listing_id,
    host_id,
    host_name,
    host_since,
    total_listings,
    total_active_listings,
    average_rating
from fact_listings
