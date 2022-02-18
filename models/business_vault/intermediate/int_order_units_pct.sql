with

order_item_details as ( select * from {{ ref('order_item_details') }} )
,sku as ( select * from {{ ref('skus') }} )

,category_reassignment as ( 
    select distinct order_id
        ,bid_id 
        ,bid_quantity
        ,iff(is_single_sku_bid_item,'BUNDLE',category) as modified_category
    from order_item_details
        left join sku on order_item_details.sku_key = sku.sku_key
)

,units_by_category as ( 
    select order_id
        ,sum(iff(modified_category = 'BEEF',bid_quantity,0)) as beef_units
        ,sum(iff(modified_category = 'BISON',bid_quantity,0)) as bison_units
        ,sum(iff(modified_category = 'CHICKEN',bid_quantity,0)) as chicken_units
        ,sum(iff(modified_category = 'DESSERTS',bid_quantity,0)) as desserts_units
        ,sum(iff(modified_category = 'DUCK',bid_quantity,0)) as duck_units
        ,sum(iff(modified_category = 'GAME MEAT',bid_quantity,0)) as game_meat_units
        ,sum(iff(modified_category = 'JAPANESE WAGYU',bid_quantity,0)) as japanese_wagyu_units
        ,sum(iff(modified_category = 'LAMB',bid_quantity,0)) as lamb_units
        ,sum(iff(modified_category = 'PET FOOD',bid_quantity,0)) as pet_food_units
        ,sum(iff(modified_category = 'PLANT-BASED PROTEINS',bid_quantity,0)) as plant_based_proteins_units
        ,sum(iff(modified_category = 'PORK',bid_quantity,0)) as pork_units
        ,sum(iff(modified_category = 'SALTS & SEASONINGS',bid_quantity,0)) as salts_seasonings_units
        ,sum(iff(modified_category = 'SEAFOOD',bid_quantity,0)) as seafood_units
        ,sum(iff(modified_category = 'STARTERS & SIDES',bid_quantity,0)) as starters_sides_units
        ,sum(iff(modified_category = 'TURKEY',bid_quantity,0)) as turkey_units
        ,sum(iff(modified_category = 'WAGYU',bid_quantity,0)) as wagyu_units
        ,sum(iff(modified_category = 'BUNDLE',bid_quantity,0)) as bundle_units
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
