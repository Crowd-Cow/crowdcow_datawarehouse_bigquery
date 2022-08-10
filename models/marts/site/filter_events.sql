with

filter_event as ( select * from {{ ref('events') }} where event_name = 'PRODUCT-FILTER-SORT-CHANGED' )

,compare_filters as (
    select
        event_id
        ,visit_id
        ,user_id
        ,occurred_at_utc
        ,from_filter
        ,to_filter
        ,parse_json(datawarehouse.compare_objects(from_filter,to_filter)) as filter_changes
    from filter_event
)

,parse_filter_changes as (
    select 
        compare_filters.event_id
        ,compare_filters.visit_id
        ,compare_filters.user_id
        ,compare_filters.occurred_at_utc
        ,modification.key
        ,modification.value::text as filter_value
        ,modification.path
        
        ,case
            when key is null then regexp_replace(path,'(added_filter|modified_filter)|([\'.\\[\\]0-9])','')
            else key
        end as path_clean
    
    from compare_filters,
        lateral flatten( input => filter_changes, recursive => true ) as modification
    where path like any ('added_filter%','modified_filter%')
        {% raw %} and not(value like any ('[%]','{%}')) {% endraw %}
)


,clean_filter_actions as (
    select distinct
        event_id
        ,visit_id
        ,user_id
        ,occurred_at_utc

        ,case 
            when path like 'added_filter%' then 'ADDED FILTER'
            when path like 'modified_filter%' then 'MODIFIED FILTER'
            else 'OTHER'
        end as filter_action

        ,case
            when path_clean like any ('%[min]%','%[max]%') then upper(replace(replace(path_clean,'[','_'),']',''))
            else upper(regexp_replace(path_clean,'[\\[\\]]',''))
         end as filter_name
        ,upper(coalesce((try_cast(filter_value as int)/100)::text,filter_value)) as filter_value
    from parse_filter_changes
)

select * from clean_filter_actions
