
with 

source as ( select * from {{ source('facebook_ads', 'facebook_ads_campaigns') }} )
,insights as ( select * from {{ source('facebook_ads', 'facebook_ads_insights') }} )

,renamed as (
  select    
      date(i.date_start) as date_start,
      c.name as campaign_name,
      sum(spend) as spend,
  from source c
  left join insights i on c.id = i.campaign_id
  group by 1,2 
  order by 2 desc
)

select * from renamed