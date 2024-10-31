with 
 postal_codes as (select * from {{ ref('fc_postal_codes') }}   )
 ,orders as (select * from {{ ref('orders') }}   )
 ,skus as (select * from {{ ref('skus') }}   )
 ,order_item_details as (select * from {{ ref('order_item_details') }}   )

,fcs_postal_codes  as (
	SELECT 
		postal_code as postal_code
		,fc_id as fc_id
		,transit_days as transit_days
		,priority as priority
		,ROW_NUMBER() OVER(PARTITION BY POSTAL_CODE ORDER BY TRANSIT_DAYS ASC, PRIORITY ASC) AS Rank
	from postal_codes as fc_postal_codes
	where 
		transit_method = 'UPS'
		and fc_id is not null 
)
,item_revenue as (
	SELECT 
		date(orders.order_paid_at_utc) as order_paid_date
		,fcs_postal_codes.fc_id as fc_name
		,skus.category as category
		,skus.sub_category as subcategory
		,skus.cut_id
		,skus.cut_name
		--,order_item_details.fc_id as original_fc 
		,sum(order_item_details.sku_quantity) as quantity_sold
		,sum(order_item_details.sku_gross_product_revenue) as gross_revenue
		,sum(order_item_details.sku_net_product_revenue) as net_revenue
	from order_item_details as order_item_details
	left join orders as orders on orders.order_id = order_item_details.order_id
	left join skus as skus on skus.sku_key = order_item_details.sku_key
	left join fcs_postal_codes as fcs_postal_codes on orders.order_delivery_postal_code = cast(fcs_postal_codes.postal_code as string) and fcs_postal_codes.rank = 1 
	where 
		order_paid_at_utc >= '2023-01-01'
		and not orders.is_cancelled_order
		and orders.is_paid_order
		and order_item_details.fc_id = 10 
	group by 1,2,3,4,5,6
)
select 
 *
,round(safe_divide(gross_revenue,quantity_sold),2) as avgerage_list_price
,round(safe_divide(net_revenue,quantity_sold),2) as average_effective_price
from item_revenue

