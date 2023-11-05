{{ config(materialized='table') }}

with property_data as (
  -- Select the staged property data with necessary transformations
  select
    property_id,
    property_type,
    property_name,
    room_type,
    amenities,
    address, 
    listing_status, 
    host_id, 
    current_timestamp as effective_date 
  from {{ ref('staging_property') }} 
)

, ranked_properties as (
  select
    *,
    row_number() over (
      partition by property_id
      order by effective_date desc
    ) as rn
  from property_data
)

select
  property_id as dim_property_key,
  property_type,
  property_name,
  room_type,
  amenities,
  address as dim_address,
  listing_status as dim_listing_status,
  host_id as dim_host_id,
  effective_date as dim_effective_date
from ranked_properties
where rn = 1 
