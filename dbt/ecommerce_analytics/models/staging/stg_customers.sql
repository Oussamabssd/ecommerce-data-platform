
WITH source AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'customers') }}

),

renamed_and_cleaned AS (

    SELECT
        CAST("customer_id" AS NUMBER(38, 0)) AS customer_id,

        NULLIF(TRIM("full_name"), '') AS full_name,

        LOWER(NULLIF(TRIM("email"), '')) AS email,

        NULLIF(TRIM("city"), '') AS city,

        NULLIF(TRIM("country"), '') AS country,

        TO_TIMESTAMP_NTZ("created_at", 9) AS created_at,

        TO_TIMESTAMP_NTZ("updated_at", 9) AS updated_at

    FROM source

)

SELECT *
FROM renamed_and_cleaned
