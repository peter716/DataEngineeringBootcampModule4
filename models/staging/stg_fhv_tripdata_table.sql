-- with 

-- source as (

--     select * from {{ source('staging', 'fhv_tripdata_table') }}

-- ),

{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
  from {{ source('staging','fhv_tripdata_table') }}
--   where date(pickup_datetime) between '2019-01-01' and '2019-12-31'
  where extract(year from pickup_datetime) = 2019
)
-- renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'affiliated_base_number']) }} as tripid,
        dispatching_base_num,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(pickup_datetime as timestamp) as dropoff_datetime,
        -- pickup_datetime,
        -- dropoff_datetime,
        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
         {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
        -- pulocationid,
        -- dolocationid,
        sr_flag,
        affiliated_base_number
from tripdata


-- )

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

