{% snapshot product_quantity_availables_ss %}

{{
   config(
       target_schema='snapshots',
       unique_key='id',

       strategy='timestamp',
       updated_at='__updatetime'
   )
}}

select * from {{ source('cc', 'product_quantity_availables') }}

{% endsnapshot %}
