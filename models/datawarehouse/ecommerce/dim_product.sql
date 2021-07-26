with stage as (

    select * from {{ ref('stg_cc_products') }}

),

base as (

  select * 
    , dbt_valid_to 
    , dbt_valid_from
  from stage

)

select * from base
