/*** Snowflake prototype dashboard: https://app.snowflake.com/us-east-1/lna65058/first-box-dashboard-includes-any-visitor-that-saw-the-homepage-with-fbq-dZ6KnqQC4 ****/
/**** Pct of all FBQ CTA that Added Bundle to Cart ****/

with invalid_experiments as (
    select
        visit_id,
        experiment_token,
        count(distinct experiment_variant) as variant_count
    from
        experiments_by_event_id
    where
        experiment_token = :experiment_token
    group by
        1,
        2
    having
        count(distinct experiment_variant) > 1
),
valid_experiments as (
    select
        distinct visit_id,
        experiment_token,
        experiment_variant
    from
        experiments_by_event_id
    where
        visit_id not in (
            select
                visit_id
            from
                invalid_experiments
        )
        and experiment_token = :experiment_token
),
members as (
    select
        user_id,
        min(created_at) as first_subscription_date,
        count(subscription_id) as subscription_count
    from
        dim_subscription
    where
        dbt_valid_to is null
    group by
        1
),
homepage_view_events as (
    select
        visit_id,
        count(event_id) as event_count
    from
        fact_event_pageview
    where
        (parse_url(url):path::text = ''
        or parse_url(url):path::text = 'l'
)
and url not like '%/?first-box%'
group by
    1
),
homepage_fbq_impressions as (
    select
        fact_visit.visit_id,
        fact_visit.user_id,case
            when dim_user.email like 'TEMPORARY%CROWDCOW.COM%' then TRUE
            when dim_user.email is null then FALSE
            else FALSE
        end as is_guest_user,
        members.user_id is not null as is_member,
        date(
            convert_timezone(
                'UTC',
                'America/Los_Angeles',
                fact_visit.visited_at
            )
        ) as visit_date,
        valid_experiments.experiment_token,
        valid_experiments.experiment_variant
    from
        fact_visit
        inner join valid_experiments on fact_visit.visit_id = valid_experiments.visit_id
        left join dim_user on fact_visit.user_id = dim_user.user_id
        and dim_user.dbt_valid_to is null
        left join members on fact_visit.user_id = members.user_id
    where
        convert_timezone(
            'UTC',
            'America/Los_Angeles',
            fact_visit.visited_at
        ) = :daterange
        and fact_visit.visit_id in (
            select
                visit_id
            from
                homepage_view_events
        )
        and not fact_visit.is_bot
        and not fact_visit.is_internal_traffic
        and valid_experiments.experiment_variant = 'experimental'
        and (
            utm_medium <> 'FIELD-MARKETING'
            or utm_medium is null
        )
        and (
            members.user_id is null
            or members.first_subscription_date >= fact_visit.visited_at
        )
),
clicked_fbq_cta as (
    select
        homepage_fbq_impressions.*
    from
        homepage_fbq_impressions
        inner join event_first_box_hero_selected on homepage_fbq_impressions.visit_id = event_first_box_hero_selected.visit_id
),
total_clicks_on_fbq_cta as (
    select
        homepage_fbq_impressions.visit_date,
        count(distinct event_first_box_hero_selected.visit_id) as total_cta_visits
    from
        homepage_fbq_impressions
        inner join event_first_box_hero_selected on homepage_fbq_impressions.visit_id = event_first_box_hero_selected.visit_id
    group by
        1
),
clicked_on_bundle as (
    select
        clicked_fbq_cta.visit_id,
        count(event_quiz_nav_purchase_bundle.visit_id) as cnt
    from
        clicked_fbq_cta
        inner join event_quiz_nav_purchase_bundle on clicked_fbq_cta.visit_id = event_quiz_nav_purchase_bundle.visit_id
    group by
        1
),
clicked_customize_bundle as (
    select
        clicked_fbq_cta.visit_id,
        count(event_quiz_nav_customize_bundle.visit_id) as cnt
    from
        clicked_fbq_cta
        inner join event_quiz_nav_customize_bundle on clicked_fbq_cta.visit_id = event_quiz_nav_customize_bundle.visit_id
    group by
        1
),
bid_items_of_bundles as (
    select
        event_quiz_nav_purchase_bundle.bid_item_key as bid_item_key
    from
        clicked_fbq_cta
        inner join event_quiz_nav_purchase_bundle on event_quiz_nav_purchase_bundle.visit_id = clicked_fbq_cta.visit_id
),
bundles_added_to_cart as (
    select
        count(distinct fact_order_item.order_id) as added_vol,
        fact_cart_event.product_name || '-' ||(
            case
                when upper(fact_cart_event.product_token) in (
                    'PBG2LZNKOTC',
                    'PONAY5JGQNF',
                    'P3HI51TEWD8',
                    'PSISNAM4LVM'
                ) then 'Family'
                when upper(fact_cart_event.product_token) in (
                    'PRUD3ZXRUA9',
                    'PK6QD8TMLUS',
                    'PFDJ0FLNDMX',
                    'P0NXK4Z3UFQ'
                ) then 'Standard'
                else 'Unknown'
            end
        ) as bundle_size,
        clicked_fbq_cta.visit_date
    from
        clicked_fbq_cta
        inner join fact_order on fact_order.visit_id = clicked_fbq_cta.visit_id
        inner join fact_order_item on fact_order_item.order_id = fact_order.order_id
        inner join fact_cart_event on fact_cart_event.order_id = fact_order.order_id
        inner join bid_items_of_bundles on bid_items_of_bundles.bid_item_key = fact_order_item.bid_item_key
    where
        fact_cart_event.event_name = 'order_add_to_cart'
        and upper(fact_cart_event.product_token) in (
            'PBG2LZNKOTC',
            'PONAY5JGQNF',
            'P3HI51TEWD8',
            'PSISNAM4LVM',
            'PRUD3ZXRUA9',
            'PK6QD8TMLUS',
            'PFDJ0FLNDMX',
            'P0NXK4Z3UFQ'
        )
    group by
        2,3
)

select
    bundle_size,
    clicked_fbq_cta.visit_date,
    bundles_added_to_cart.added_vol,
    total_clicks_on_fbq_cta.total_cta_visits ,coalesce((bundles_added_to_cart.added_vol::float/total_clicks_on_fbq_cta.total_cta_visits::float),0) as pct_of_cta_clicks_added
from
    clicked_fbq_cta
    inner join bundles_added_to_cart on bundles_added_to_cart.visit_date = clicked_fbq_cta.visit_date
    inner join total_clicks_on_fbq_cta on total_clicks_on_fbq_cta.visit_date = clicked_fbq_cta.visit_date
group by
    1,2,3,4
order by
    2,1;
    