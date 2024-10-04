with

source as ( select * from {{ source('google_sheets', 'approved_invoice_lot_mapping') }} )

,renamed as (
    select
        {{ clean_strings('cast(lot_number as string)') }} as lot_number
        ,{{ clean_strings('cast(invoice_number as string)') }} as invoice_number
        ,{{ clean_strings('cast(vendor_id as string)') }} as vendor_id
    from source
)

select 
    {{ dbt_utils.surrogate_key(['invoice_number','vendor_id']) }} as invoice_key
    ,*
from renamed
