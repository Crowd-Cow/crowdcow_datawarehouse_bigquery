with
order_item_details as ( select * from {{ ref('order_item_details') }} )
, skus as ( select * from {{ ref('skus') }} )

,category_reassignment as ( 
    select distinct order_id
        ,bid_id 
        ,bid_quantity
        ,case when NOT is_single_sku_bid_item then 'BUNDLE' else category end as modified_category
    from order_item_details
    left join skus on skus.sku_key = order_item_details.sku_key
)

,units_by_category as ( 
    select order_id
        ,sum(case when modified_category = 'BEEF' then bid_quantity else 0 end) as beef_units
        ,sum(case when modified_category = 'BISON' then bid_quantity else 0 end) as bison_units
        ,sum(case when modified_category = 'CHICKEN' then bid_quantity else 0 end) as chicken_units
        ,sum(case when modified_category = 'DESSERTS' then bid_quantity else 0 end) as desserts_units
        ,sum(case when modified_category = 'DUCK' then bid_quantity else 0 end) as duck_units
        ,sum(case when modified_category = 'GAME MEAT' then bid_quantity else 0 end) as game_meat_units
        ,sum(case when modified_category = 'JAPANESE WAGYU' then bid_quantity else 0 end) as japanese_wagyu_units
        ,sum(case when modified_category = 'LAMB' then bid_quantity else 0 end) as lamb_units
        ,sum(case when modified_category = 'PET FOOD' then bid_quantity else 0 end) as pet_food_units
        ,sum(case when modified_category = 'PLANT-BASED PROTEINS' then bid_quantity else 0 end) as plant_based_proteins_units
        ,sum(case when modified_category = 'PORK' then bid_quantity else 0 end) as pork_units
        ,sum(case when modified_category = 'SALTS & SEASONINGS' then bid_quantity else 0 end) as salts_seasonings_units
        ,sum(case when modified_category = 'SEAFOOD' then bid_quantity else 0 end) as seafood_units
        ,sum(case when modified_category = 'STARTERS & SIDES' then bid_quantity else 0 end) as starters_sides_units
        ,sum(case when modified_category = 'TURKEY' then bid_quantity else 0 end) as turkey_units
        ,sum(case when modified_category = 'WAGYU' then bid_quantity else 0 end) as wagyu_units
        ,sum(case when modified_category = 'BUNDLE' then bid_quantity else 0 end) as bundle_units
        ,sum(bid_quantity) as total_units
    from category_reassignment
    group by 1
)

,pct_category as (
    select units_by_category.*
        ,div0(beef_units,total_units) as pct_beef
        ,div0(bison_units,total_units) as pct_bison
        ,div0(chicken_units,total_units) as pct_chicken
        ,div0(desserts_units, total_units) as pct_desserts
        ,div0(duck_units,total_units) as pct_duck
        ,div0(game_meat_units,total_units) as pct_game_meat
        ,div0(japanese_wagyu_units,total_units) as pct_japanese_wagyu
        ,div0(lamb_units,total_units) as pct_lamb
        ,div0(pet_food_units,total_units) as pct_pet_food
        ,div0(plant_based_proteins_units,total_units) as pct_plant_based_proteins
        ,div0(pork_units,total_units) as pct_pork
        ,div0(salts_seasonings_units,total_units) as pct_salts_seasonings
        ,div0(seafood_units,total_units) as pct_seafood
        ,div0(starters_sides_units,total_units) as pct_starters_sides
        ,div0(turkey_units,total_units) as pct_turkey
        ,div0(wagyu_units,total_units) as pct_wagyu
        ,div0(bundle_units,total_units) as pct_bundle
    from units_by_category
)

select * from pct_category