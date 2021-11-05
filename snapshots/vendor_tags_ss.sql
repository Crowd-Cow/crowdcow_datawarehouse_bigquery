{% snapshot vendor_tags_ss %}

{{
   config(
       target_schema='snapshots',
       unique_key='id',

       strategy='timestamp',
       updated_at='updated_at'
   )
}}

select * from {{ source('cc', 'vendor_tags') }}

{% endsnapshot %}