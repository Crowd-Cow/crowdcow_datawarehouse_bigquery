  
 with 

source as ( select * from {{ source('google_ads', 'ad_group_custom_parameter_history') }} )

,renamed as ( 
    select 
        ad_group_id
        ,updated_at as updated_at_utc
        ,updated_at::date as ad_group_parameter_valid_from_date
        ,ifnull(lead(updated_at::date,1) over(partition by ad_group_id,sequence_id order by updated_at),'2999-01-01') as ad_group_parameter_valid_to_date
        ,case when key = 'adgroup' then {{ clean_strings('value') }} end as ad_group_parameter_name
        ,case when key = 'adgroupid' then value end as ad_group_parameter_id
    from source
)

select * from renamed