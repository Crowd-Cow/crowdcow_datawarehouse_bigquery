{% snapshot bid_item_sku_with_quantities_ss %}

{{
   config(
       target_schema='snapshots',
       unique_key='id',

       strategy='timestamp',
       updated_at='updated_at'
   )
}}

select * from {{ source('cc', 'bid_item_sku_with_quantities') }}

{% endsnapshot %}
