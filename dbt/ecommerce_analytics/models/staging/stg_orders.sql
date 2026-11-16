
WITH source AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'orders') }}

)

SELECT
    CAST("order_id" AS NUMBER(38, 0)) AS order_id,

    NULLIF(TRIM("order_number"), '') AS order_number,

    CAST("customer_id" AS NUMBER(38, 0)) AS customer_id,

    UPPER(NULLIF(TRIM("order_status"), '')) AS order_status,

    TO_TIMESTAMP_NTZ("order_date", 9) AS order_date,

    TO_TIMESTAMP_NTZ("updated_at", 9) AS updated_at

FROM source
