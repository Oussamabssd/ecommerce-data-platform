
WITH inventory AS (

    SELECT *
    FROM {{ ref('stg_inventory') }}

)

SELECT
    inventory_id,

    MD5(TO_VARCHAR(product_id)) AS product_key,
    product_id,

    MD5(TO_VARCHAR(warehouse_id)) AS warehouse_key,
    warehouse_id,

    quantity_available,
    updated_at

FROM inventory
