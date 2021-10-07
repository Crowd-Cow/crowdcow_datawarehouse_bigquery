{% snapshot users_ss %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',

      strategy='check',
      check_cols='all',
    )
}}

select * from {{ source('cc', 'users') }}

{% endsnapshot %}