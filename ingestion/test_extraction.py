from sqlalchemy import text
from ingestion.db import get_engine


def test_extraction():
    engine = get_engine()

    query = text(
        """
        SELECT
            o.order_number,
            c.full_name AS customer,
            o.order_status,
            o.order_date
        FROM orders o
        JOIN customers c
            ON o.customer_id = c.customer_id
        ORDER BY o.order_date;
    """
    )

    try:
        with engine.connect() as connection:
            rows = connection.execute(query).fetchall()

            print(f"Successfully extracted {len(rows)} orders.")

            for row in rows:
                print(row)

    finally:
        engine.dispose()


if __name__ == "__main__":
    test_extraction()
