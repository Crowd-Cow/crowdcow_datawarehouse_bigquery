WITH
  pipeline_receivables AS (SELECT * FROM {{ ref('pipeline_receivables') }}),
  skus AS (SELECT * FROM {{ ref('skus') }}),
  fcs AS (SELECT * FROM {{ ref('fcs') }})

SELECT
  pipeline_receivables.RECEIVABLE_STATUS AS receivable_status,
  pipeline_receivables.FARM_OUT_NAME AS pipeline_receivables_farm_name,
  skus.sku_name AS sku_name,
  skus.CATEGORY AS category,
  skus.SUB_CATEGORY AS sub_category,
  fcs.FC_NAME AS fc_name,
  skus.FARM_NAME AS farm_name,
  FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(DATE(pipeline_receivables.FC_SCAN_PROPOSED_DATE, 'America/Los_Angeles'), WEEK)) AS fc_scan_proposed_week,
  pipeline_receivables.LOT_NUMBER AS lot_number,
  FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(pipeline_receivables.FC_SCAN_PROPOSED_DATE, 'America/Los_Angeles'), MONTH)) AS fc_scan_proposed_month,
  COALESCE(SUM(pipeline_receivables.quantity_ordered), 0) AS quantity_ordered,
  COALESCE(SUM(CASE
    WHEN pipeline_receivables.IS_DESTROYED IS NULL OR pipeline_receivables.IS_DESTROYED = FALSE THEN pipeline_receivables.quantity_ordered
    ELSE NULL
  END), 0) AS non_destroyed_receivables
FROM PIPELINE_RECEIVABLES AS pipeline_receivables
LEFT JOIN SKUS AS skus ON pipeline_receivables.sku_key = skus.sku_key
LEFT JOIN FCS AS fcs ON pipeline_receivables.FC_ID = fcs.FC_ID
  AND fcs.DBT_VALID_TO IS NULL
WHERE pipeline_receivables.FC_SCAN_PROPOSED_DATE >= TIMESTAMP(
  DATE_SUB(DATE_TRUNC(CURRENT_DATE('America/Los_Angeles'), WEEK), INTERVAL 91 DAY), 'America/Los_Angeles'
)
  AND (pipeline_receivables.is_rastellis IS NULL OR pipeline_receivables.is_rastellis = FALSE)
GROUP BY
  receivable_status,
  pipeline_receivables_farm_name,
  sku_name,
  category,
  sub_category,
  fc_name,
  farm_name,
  fc_scan_proposed_week,
  lot_number,
  fc_scan_proposed_month
ORDER BY
  fc_scan_proposed_week DESC