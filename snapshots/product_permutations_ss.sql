{% snapshot product_permutations_ss %}

{{
   config(
       enabled=false,
       target_schema='snapshots',
       unique_key='id',

       strategy='timestamp',
       updated_at='updated_at'
   )
}}

select * from {{ source('cc', 'product_permutations') }}

{% endsnapshot %}
