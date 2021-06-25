{% snapshot orders_snapshot %}

{{
    config(
      target_database='raw', 
      target_schema='snapshots',
      unique_key='id',

      strategy='check',
      check_cols='all',
    )
}}

select * from {{ source('cc', 'users') }}

{% endsnapshot %}