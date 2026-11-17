
WITH products AS (

    SELECT *
    FROM {{ ref('stg_products') }}

),

suppliers AS (

    SELECT *
    FROM {{ ref('stg_suppliers') }}

)

SELECT
    MD5(TO_VARCHAR(p.product_id)) AS product_key,

    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    p.supplier_id,

    s.supplier_name,
    s.country AS supplier_country,

    p.unit_price,
    p.created_at,
    p.updated_at

FROM products p

LEFT JOIN suppliers s
    ON p.supplier_id = s.supplier_id
