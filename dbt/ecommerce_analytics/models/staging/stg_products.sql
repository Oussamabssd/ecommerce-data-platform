
WITH source AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'products') }}

)

SELECT
    CAST("product_id" AS NUMBER(38, 0)) AS product_id,

    UPPER(NULLIF(TRIM("sku"), '')) AS sku,

    NULLIF(TRIM("product_name"), '') AS product_name,

    NULLIF(TRIM("category"), '') AS category,

    CAST("supplier_id" AS NUMBER(38, 0)) AS supplier_id,

    CAST("unit_price" AS NUMBER(12, 2)) AS unit_price,

    TO_TIMESTAMP_NTZ("created_at", 9) AS created_at,

    TO_TIMESTAMP_NTZ("updated_at", 9) AS updated_at

FROM source
