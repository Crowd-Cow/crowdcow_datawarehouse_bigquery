
with
campaign as ( select * From {{ ref('stg_iterable__campaign_history') }} )

select * from campaign
