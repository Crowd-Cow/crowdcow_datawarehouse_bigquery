{% snapshot product_variants_ss %}

{{
   config(
       target_schema='snapshots',
       unique_key='id',

       strategy='timestamp',
       updated_at='updated_at'
   )
}}

select * from {{ source('cc', 'product_variants') }}

{% endsnapshot %}
