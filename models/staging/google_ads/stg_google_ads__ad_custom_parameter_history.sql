{{ config(
  enabled=false
) }}
with 

source as ( select * from {{ source('google_ads', 'ad_custom_parameter_history') }} )

,renamed as (

    select
        ad_group_id
        ,ad_id
        ,sequence_id
        ,updated_at as updated_at_utc
        ,updated_at::date as parameter_valid_from_date
        ,ifnull(lead(updated_at::date,1) over(partition by ad_group_id, ad_id, sequence_id order by updated_at),'2999-01-01') as parameter_valid_to_date
        ,{{ clean_strings('value') }} as value
    from source
    where key = 'ad'
)

select * from renamed

