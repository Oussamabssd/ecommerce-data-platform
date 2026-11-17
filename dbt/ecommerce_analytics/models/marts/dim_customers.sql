
WITH customers AS (

    SELECT *
    FROM {{ ref('stg_customers') }}

)

SELECT
    MD5(TO_VARCHAR(customer_id)) AS customer_key,
    customer_id,
    full_name,
    email,
    city,
    country,
    created_at,
    updated_at

FROM customers
