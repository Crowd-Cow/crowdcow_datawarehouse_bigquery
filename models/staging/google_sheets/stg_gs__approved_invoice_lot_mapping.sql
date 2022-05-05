with

source as ( select * from {{ source('google_sheets', 'approved_invoice_lot_mapping') }} )

,renamed as (
    select
        {{ clean_strings('lot_number::text') }} as lot_number
        ,{{ clean_strings('invoice_number::text') }} as invoice_number
        ,{{ clean_strings('vendor_id::text') }} as vendor_id
    from source
)

select 
    {{ dbt_utils.surrogate_key(['invoice_number','vendor_id']) }} as invoice_key
    ,*
from renamed
