{{ config(
  enabled=false
) }}
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
        ,dbolivar_analytics_dev_business_vault.compare_objects(TO_JSON_STRING(from_filter), TO_JSON_STRING(to_filter)) as filter_changes
        ,on_page_url
        ,on_page_path
    from filter_event
)

,parse_filter_changes as (
    select 
        compare_filters.event_id
        ,compare_filters.visit_id
        ,compare_filters.user_id
        ,compare_filters.occurred_at_utc
        ,compare_filters.on_page_url
        ,compare_filters.on_page_path
        ,json_extract_scalar(modification, '$.key') as key
        ,json_extract_scalar(modification, '$.value') as filter_value
        ,json_extract_scalar(modification, '$.path') as path
        
        ,case
            when json_extract_scalar(modification, '$.key') is null then regexp_replace(json_extract_scalar(modification, '$.path'), r'(added_filter|modified_filter)|([\'\\.\\[\\]0-9])','')
            else json_extract_scalar(modification, '$.key')
        end as path_clean
    
    from compare_filters,
    unnest(json_extract_array(filter_changes)) as modification
    where regexp_contains(json_extract_scalar(modification, '$.path'), r'added_filter|modified_filter')
        {% raw %} and not(regexp_contains(json_extract_scalar(modification, '$.value'), r'\\[.*\\]|\\{.*\\}')) {% endraw %}
)

,clean_filter_actions as (
    select distinct
        event_id
        ,visit_id
        ,user_id
        ,occurred_at_utc
        ,on_page_url
        ,on_page_path

        ,case 
            when regexp_contains(path, r'added_filter%') then 'ADDED FILTER'
            when regexp_contains(path, r'modified_filter%') then 'MODIFIED FILTER'
            else 'OTHER'
        end as filter_action

        ,case
            when regexp_contains(path_clean, r'\\[min\\]|\\[max\\]') then upper(replace(replace(path_clean,'[','_'),']',''))
            else upper(regexp_replace(path_clean,'[\\[\\]]',''))
         end as filter_name
        ,upper(coalesce(cast(cast(filter_value as numeric)/100 as string),filter_value)) as filter_value
    from parse_filter_changes
)

select * from clean_filter_actions