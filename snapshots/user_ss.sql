{% snapshot orders_snapshot %}

{{
    config(
      target_database='bi_snapshots',
      target_schema='snapshots',
      unique_key='id',

      strategy='check',
      check_cols='all',
    )
}}

select * from {{ source('cc', 'users') }}

{% endsnapshot %}