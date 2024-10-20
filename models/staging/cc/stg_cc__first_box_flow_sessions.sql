with source as (

    select * from {{ source('cc','first_box_flow_sessions') }} where __deleted is null

),

renamed as (

    select
        id as id
        ,created_at as created_at_utc
        ,entered_at as entered_at_utc
        ,updated_at as updated_at_utc
        ,completed_at as completed_at_utc
        ,exited_at as exited_at_utc
        ,user_id as user_id
        ,renew_period_type as renew_period_type
        ,order_id as order_id 

    from source

)

select * from renamed

