
with source as (

    select * from {{ source('cc', 'product_sales_channel_lists') }}

),

renamed as (

  select
    id as product_sales_channel_lists_id
    , product_id
    , created_at as created_at_utc
    , updated_at as updated_at_utc
    , website as is_website
    , google_feed as is_google_feed
    , wholesale as is_wholesale
    , clearance as is_clearance
    , add_on as is_add_on
    , subscription as is_subscription
    , _fivetran_synced
    , _fivetran_deleted

  from source

)

select * from renamed
