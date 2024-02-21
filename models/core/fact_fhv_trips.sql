{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata_table as (
    select *, 
        'Fhv' as service_type
    from {{ ref('stg_fhv_tripdata_table') }}
    where pickup_locationid is not null and dropoff_locationid is not null  
), 
fhv_table as (
    select * from fhv_tripdata_table
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
 select
    fhv_table.tripid,
    fhv_table.dispatching_base_num,
    fhv_table.pickup_datetime,
    fhv_table.dropoff_datetime,
    fhv_table.pickup_locationid,
    fhv_table.dropoff_locationid,
    fhv_table.sr_flag,
    fhv_table.affiliated_base_number,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone 
from fhv_table
inner join dim_zones as pickup_zone
on fhv_table.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_table.dropoff_locationid = dropoff_zone.locationid

-- 