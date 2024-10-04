with 

source as ( select * from {{ source('acumatica', 'acumatica_bills') }} )
/*,details as ( select * from 'acumatica_bills_details')

,flatten_details as (
    select
        __acumatica_bills_panoply_id,
        CAST(JSON_EXTRACT_SCALAR(Account, '$.value') AS INT64) AS account_nbr,
        CAST(JSON_EXTRACT_SCALAR(Amount, '$.value') AS FLOAT64) AS line_item_amount,
        JSON_EXTRACT_SCALAR(Branch, '$.value') AS branch,
        JSON_EXTRACT_SCALAR(Description, '$.value') AS line_item_description,
        CAST(JSON_EXTRACT_SCALAR(ExtendedCost, '$.value') AS FLOAT64) AS line_item_extended_cost,
        CAST(JSON_EXTRACT_SCALAR(Qty, '$.value') AS FLOAT64) AS line_item_quantity,
        CAST(JSON_EXTRACT_SCALAR(Subaccount, '$.value') AS INT64) AS sub_account,
        JSON_EXTRACT_SCALAR(TransactionDescription, '$.value') AS transaction_description,
        CAST(JSON_EXTRACT_SCALAR(UnitCost, '$.value') AS FLOAT64) AS unit_cost,
        --coalesce(JSON_EXTRACT_SCALAR(note, '$.value'),null) AS line_item_note, 
        rowNumber AS line_number
    from details
        
)*/

, flatten_details AS (
    SELECT
        source.*,
        CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.Account.value') AS INT64) AS account_nbr,
        CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.Amount.value') AS FLOAT64) AS line_item_amount,
        JSON_EXTRACT_SCALAR(flattened_element, '$.Branch.value') AS branch,
        JSON_EXTRACT_SCALAR(flattened_element, '$.Description.value') AS line_item_description,
        CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.ExtendedCost.value') AS FLOAT64) AS line_item_extended_cost,
        CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.Qty.value') AS FLOAT64) AS line_item_quantity,
        CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.Subaccount.value') AS INT64) AS sub_account,
        JSON_EXTRACT_SCALAR(flattened_element, '$.TransactionDescription.value') AS transaction_description,
        CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.UnitCost.value') AS FLOAT64) AS unit_cost,
        JSON_EXTRACT_SCALAR(flattened_element, '$.note') AS line_item_note,
        CAST(JSON_EXTRACT_SCALAR(flattened_element, '$.rowNumber') AS INT64) AS line_number
    FROM
        source,
        UNNEST(JSON_EXTRACT_ARRAY(details)) AS flattened_element
)

, renamed AS (
    SELECT
        cast({{ dbt_utils.surrogate_key(['id','line_number']) }} as string) AS bill_item_id,
        {{ dbt_utils.surrogate_key(['vendor_ref', 'vendor']) }} AS invoice_key,
        id AS bill_id,
        date AS bill_date_utc,
        {{ clean_strings('note') }} AS notes,
        due_date AS due_date_utc,
        cash_account,
        {{ clean_strings('description') }} AS bill_description,
        {{ clean_strings('type') }} AS bill_type,
        amount AS bill_amount,
        reference_number,
        location_id,
        hold AS is_on_hold,
        {{ clean_strings('terms') }} AS terms,
        vendor,
        approved_for_payement AS is_approved_for_payment,
        vendor_ref,
        {{ clean_strings('currency_id') }} AS currency_id,
        post_period,
        {{ clean_strings('status') }} AS bill_status,
        account_nbr,
        line_item_amount,
        {{ clean_strings('branch') }} AS branch,
        {{ clean_strings('line_item_description') }} AS line_item_description,
        line_item_extended_cost,
        line_item_quantity,
        sub_account,
        {{ clean_strings('transaction_description') }} AS transaction_description,
        unit_cost,
        {{ clean_strings('line_item_note') }} AS line_item_note,
        line_number
    FROM
        flatten_details
)

SELECT * FROM renamed