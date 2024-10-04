{{ config(
  enabled=false
) }}
with 

source as ( select * from {{ source('google_ads', 'ad_group_custom_parameter_history') }} )

,renamed as ( 
    select 
        source.ad_group_id
        ,source.updated_at as updated_at_utc
        ,max(if(key = 'adgroup',{{ clean_strings('value') }},null)) as ad_group_parameter
        ,max(if(key = 'adgroupid',{{ clean_strings('value') }},null)) as ad_group_id_parameter
    from source
    group by 1, 2

)

select * from renamed