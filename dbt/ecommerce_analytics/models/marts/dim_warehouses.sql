
WITH warehouses AS (

    SELECT *
    FROM {{ ref('stg_warehouses') }}

)

SELECT
    MD5(TO_VARCHAR(warehouse_id)) AS warehouse_key,

    warehouse_id,
    warehouse_code,
    warehouse_name,
    city,
    country,
    created_at

FROM warehouses
