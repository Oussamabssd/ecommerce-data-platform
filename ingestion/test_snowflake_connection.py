from ingestion.snowflake_db import get_snowflake_connection


def test_snowflake_connection():
    connection = None

    try:
        print("Connecting to Snowflake...")

        connection = get_snowflake_connection()

        print("Connection successful!")

        query = """
            SELECT
                CURRENT_USER() AS USER_NAME,
                CURRENT_ROLE() AS ROLE_NAME,
                CURRENT_WAREHOUSE() AS WAREHOUSE_NAME,
                CURRENT_DATABASE() AS DATABASE_NAME,
                CURRENT_SCHEMA() AS SCHEMA_NAME,
                CURRENT_VERSION() AS SNOWFLAKE_VERSION
        """

        cursor = connection.cursor()

        try:
            cursor.execute(query)

            result = cursor.fetchone()
            columns = [column[0] for column in cursor.description]

            print("\nSnowflake configuration:")

            for column, value in zip(columns, result):
                print(f"{column}: {value}")

        finally:
            cursor.close()

    except Exception as error:
        print(f"Snowflake connection failed: {error}")
        raise

    finally:
        if connection:
            connection.close()
            print("\nConnection closed.")


if __name__ == "__main__":
    test_snowflake_connection()
