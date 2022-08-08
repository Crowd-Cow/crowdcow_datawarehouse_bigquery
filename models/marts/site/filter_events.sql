with

filter_event as ( select * from {{ ref('events') }} where event_name = 'PRODUCT-FILTER-SORT-CHANGED' )

,hash_filters as (
    select
        event_id
        ,occurred_at_utc
        ,visit_id
        ,user_id
        ,event_name
        ,from_filter
        ,to_filter
        ,md5(hash(object_keys(from_filter))) as from_key_hash
        ,md5(hash(object_keys(to_filter))) as to_key_hash
        ,md5(hash(from_filter)) as from_hash
        ,md5(hash(to_filter)) as to_hash
    from filter_event
)

,split_out_filters as (
    select
        coalesce(from_filters.event_id,to_filters.event_id) as event_id
        ,coalesce(from_filters.occurred_at_utc,to_filters.occurred_at_utc) as occurrred_at_utc
        ,coalesce(from_filters.visit_id,to_filters.visit_id) as visit_id
        ,coalesce(from_filters.user_id,to_filters.user_id) as user_id
        ,coalesce(from_filters.event_name,to_filters.event_name) as event_name
        ,coalesce(from_filters.did_filter_value_change,to_filters.did_filter_value_change) as did_filter_value_change
        ,coalesce(from_filters.did_filter_key_change,to_filters.did_filter_key_change) as did_filter_key_change
        ,from_filters.from_filter_key
        ,from_filters.from_filter_value
        ,to_filters.to_filter_key
        ,to_filters.to_filter_value
    from 
    (
        select
            event_id
            ,occurred_at_utc
            ,visit_id
            ,user_id
            ,event_name
            ,from_key_hash <> to_key_hash as did_filter_key_change
            ,from_hash <> to_hash as did_filter_value_change
            ,from_filter
            ,to_filter
            ,regexp_replace(frm.path::text,'[\\[\\]\']','') as from_filter_key
            ,frm.value::text as from_filter_value
        from hash_filters,
            lateral flatten( input => from_filter) frm
    ) as from_filters
    
    full outer join 
    
    (
        select
            event_id
            ,occurred_at_utc
            ,visit_id
            ,user_id
            ,event_name
            ,from_key_hash <> to_key_hash as did_filter_key_change
            ,from_hash <> to_hash as did_filter_value_change
            ,from_filter
            ,to_filter
            ,regexp_replace(to_f.path::text,'[\\[\\]\']','') as to_filter_key
            ,to_f.value::text as to_filter_value
        from hash_filters,
            lateral flatten( input => to_filter) to_f
    ) as to_filters 
    
    on from_filters.event_id = to_filters.event_id
            and from_filters.from_filter_key = to_filters.to_filter_key
)

select * from split_out_filters
