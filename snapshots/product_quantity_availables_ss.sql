{% snapshot product_quantity_availables_ss %}

{{
   config(
       target_schema='snapshots',
       unique_key='id',

       strategy='timestamp',
       updated_at='_fivetran_synced'
   )
}}

select * from {{ source('cc', 'product_quantity_availables') }}

{% endsnapshot %}
