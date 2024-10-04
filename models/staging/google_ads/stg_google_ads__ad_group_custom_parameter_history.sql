  
 with 

source as ( select * from {{ source('google_ads', 'ad_group_custom_parameter_history') }} )

,renamed as ( 
    select 
        source.ad_group_id
        ,source.updated_at as updated_at_utc
        ,max(iff(key = 'adgroup',{{ clean_strings('value') }},null)) as ad_group_parameter
        ,max(iff(key = 'adgroupid',{{ clean_strings('value') }},null)) as ad_group_id_parameter
    from source
    group by 1, 2

)

select * from renamed