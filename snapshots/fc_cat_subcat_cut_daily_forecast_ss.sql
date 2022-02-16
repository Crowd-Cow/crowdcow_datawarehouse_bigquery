{% snapshot fc_cat_subcat_cut_daily_forecast_ss %}

{{
   config(
       target_schema='snapshots',
       unique_key='id',

       strategy='check',
       check_cols = 'all'
   )
}}

select 
    {{ dbt_utils.surrogate_key(['item_id','date']) }} as id
    ,* 
from {{ source('forecast', 'fc_cat_subcat_cut_daily_forecasts') }}

{% endsnapshot %}