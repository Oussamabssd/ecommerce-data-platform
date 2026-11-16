
WITH source AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'suppliers') }}

)

SELECT
    CAST("supplier_id" AS NUMBER(38, 0)) AS supplier_id,

    NULLIF(TRIM("supplier_name"), '') AS supplier_name,

    LOWER(NULLIF(TRIM("contact_email"), '')) AS contact_email,

    NULLIF(TRIM("country"), '') AS country,

    TO_TIMESTAMP_NTZ("created_at", 9) AS created_at

FROM source
