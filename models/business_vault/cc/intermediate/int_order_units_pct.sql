with

order_item_details as ( select * from {{ ref('order_item_details') }} )
,sku as ( select * from {{ ref('skus') }} )

,category_reassignment as ( 
    select distinct 
        order_item_details.order_id
        ,order_item_details.sku_id 
        ,order_item_details.sku_quantity
        ,order_item_details.total_sku_weight
        ,order_item_details.sku_net_product_revenue
        ,if(not order_item_details.is_single_sku_bid_item,'BUNDLE',sku.category) as modified_category
    from order_item_details
        left join sku on order_item_details.sku_key = sku.sku_key
)

,units_by_category as ( 
    select order_id
        ,sum(if(modified_category = 'BEEF',sku_quantity,0)) as beef_units
        ,sum(if(modified_category = 'BISON',sku_quantity,0)) as bison_units
        ,sum(if(modified_category = 'CHICKEN',sku_quantity,0)) as chicken_units
        ,sum(if(modified_category = 'DESSERTS',sku_quantity,0)) as desserts_units
        ,sum(if(modified_category = 'DUCK',sku_quantity,0)) as duck_units
        ,sum(if(modified_category = 'GAME MEAT',sku_quantity,0)) as game_meat_units
        ,sum(if(modified_category = 'JAPANESE WAGYU',sku_quantity,0)) as japanese_wagyu_units
        ,sum(if(modified_category = 'LAMB',sku_quantity,0)) as lamb_units
        ,sum(if(modified_category = 'PET FOOD',sku_quantity,0)) as pet_food_units
        ,sum(if(modified_category = 'PLANT-BASED PROTEINS',sku_quantity,0)) as plant_based_proteins_units
        ,sum(if(modified_category = 'PORK',sku_quantity,0)) as pork_units
        ,sum(if(modified_category = 'SALTS & SEASONINGS',sku_quantity,0)) as salts_seasonings_units
        ,sum(if(modified_category = 'SEAFOOD',sku_quantity,0)) as seafood_units
        ,sum(if(modified_category = 'STARTERS & SIDES',sku_quantity,0)) as starters_sides_units
        ,sum(if(modified_category = 'TURKEY',sku_quantity,0)) as turkey_units
        ,sum(if(modified_category = 'WAGYU',sku_quantity,0)) as wagyu_units
        ,sum(if(modified_category = 'BUNDLE',sku_quantity,0)) as bundle_units
        ,sum(sku_quantity) as total_units
        ,sum(total_sku_weight) as total_product_weight
        
        ,sum(if(modified_category = 'BEEF',sku_net_product_revenue,0)) as beef_revenue
        ,sum(if(modified_category = 'BISON',sku_net_product_revenue,0)) as bison_revenue
        ,sum(if(modified_category = 'CHICKEN',sku_net_product_revenue,0)) as chicken_revenue
        ,sum(if(modified_category = 'DESSERTS',sku_net_product_revenue,0)) as desserts_revenue
        ,sum(if(modified_category = 'DUCK',sku_net_product_revenue,0)) as duck_revenue
        ,sum(if(modified_category = 'GAME MEAT',sku_net_product_revenue,0)) as game_meat_revenue
        ,sum(if(modified_category = 'JAPANESE WAGYU',sku_net_product_revenue,0)) as japanese_wagyu_revenue
        ,sum(if(modified_category = 'LAMB',sku_net_product_revenue,0)) as lamb_revenue
        ,sum(if(modified_category = 'PET FOOD',sku_net_product_revenue,0)) as pet_food_revenue
        ,sum(if(modified_category = 'PLANT-BASED PROTEINS',sku_net_product_revenue,0)) as plant_based_proteins_revenue
        ,sum(if(modified_category = 'PORK',sku_net_product_revenue,0)) as pork_revenue
        ,sum(if(modified_category = 'SALTS & SEASONINGS',sku_net_product_revenue,0)) as salts_seasonings_revenue
        ,sum(if(modified_category = 'SEAFOOD',sku_net_product_revenue,0)) as seafood_revenue
        ,sum(if(modified_category = 'STARTERS & SIDES',sku_net_product_revenue,0)) as starters_sides_revenue
        ,sum(if(modified_category = 'TURKEY',sku_net_product_revenue,0)) as turkey_revenue
        ,sum(if(modified_category = 'WAGYU',sku_net_product_revenue,0)) as wagyu_revenue
        ,sum(if(modified_category = 'BUNDLE',sku_net_product_revenue,0)) as bundle_revenue
    from category_reassignment
    group by 1
)

,pct_category as (
    select units_by_category.*
        ,safe_divide(beef_units,total_units) as pct_beef
        ,safe_divide(bison_units,total_units) as pct_bison
        ,safe_divide(chicken_units,total_units) as pct_chicken
        ,safe_divide(desserts_units, total_units) as pct_desserts
        ,safe_divide(duck_units,total_units) as pct_duck
        ,safe_divide(game_meat_units,total_units) as pct_game_meat
        ,safe_divide(japanese_wagyu_units,total_units) as pct_japanese_wagyu
        ,safe_divide(lamb_units,total_units) as pct_lamb
        ,safe_divide(pet_food_units,total_units) as pct_pet_food
        ,safe_divide(plant_based_proteins_units,total_units) as pct_plant_based_proteins
        ,safe_divide(pork_units,total_units) as pct_pork
        ,safe_divide(salts_seasonings_units,total_units) as pct_salts_seasonings
        ,safe_divide(seafood_units,total_units) as pct_seafood
        ,safe_divide(starters_sides_units,total_units) as pct_starters_sides
        ,safe_divide(turkey_units,total_units) as pct_turkey
        ,safe_divide(wagyu_units,total_units) as pct_wagyu
        ,safe_divide(bundle_units,total_units) as pct_bundle
    from units_by_category
)

select * from pct_category
