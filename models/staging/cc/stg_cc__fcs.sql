with source as (

    select * from {{ ref('fcs_ss') }} where (_fivetran_deleted is null or _fivetran_deleted = false)

),

renamed as (

    select
        id as fc_id
        ,dbt_scd_id as fc_key
        ,{{ clean_strings('name') }} as fc_name
        ,{{ clean_strings('street_address_1') }} as fc_address_1
        ,{{ clean_strings('street_address_2') }} as fc_address_2
        ,{{ clean_strings('city') }} as fc_city
        ,{{ clean_strings('state') }} as fc_state
        ,postal_code as fc_postal_code
        ,{{ clean_strings('key') }} as fc_short_name
        ,{{ clean_strings('pretty_address_notes') }} as pretty_address_notes
        ,created_at as created_at_utc
        ,longitude as fc_longitude
        ,latitude as fc_latitude        
        ,{{ clean_strings('email_address') }} as fc_email_address
        ,{{ clean_strings('region') }} as fc_region
        --,clean_strings('phone_number') }} as fc_phone_number
        ,timezone_offset_from_pst as fc_timezone_offset_from_pst
        ,updated_at as updated_at_utc
        ,in_service as is_in_service
        ,display as is_displayed
        ,third_party as is_third_party
        ,dbt_valid_to
        ,dbt_valid_from
        ,case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
        end as adjusted_dbt_valid_from
        ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from source

)

select * from renamed

