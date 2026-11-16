
WITH source AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'warehouses') }}

)

SELECT
    CAST("warehouse_id" AS NUMBER(38, 0)) AS warehouse_id,

    UPPER(NULLIF(TRIM("warehouse_code"), '')) AS warehouse_code,

    NULLIF(TRIM("warehouse_name"), '') AS warehouse_name,

    NULLIF(TRIM("city"), '') AS city,

    NULLIF(TRIM("country"), '') AS country,

    TO_TIMESTAMP_NTZ("created_at", 9) AS created_at

FROM source
