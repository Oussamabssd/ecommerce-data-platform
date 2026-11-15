from sqlalchemy import text

from ingestion.db import get_engine


def test_connection():
    engine = get_engine()

    try:
        with engine.connect() as connection:

            result = connection.execute(
                text(
                    """
                    SELECT
                        current_database() AS database_name,
                        current_user AS username,
                        version() AS postgres_version;
                """
                )
            )

            row = result.fetchone()

            print("Connection successful!")
            print(f"Database: {row.database_name}")
            print(f"User: {row.username}")
            print(f"Version: {row.postgres_version}")

    except Exception as error:
        print(f"Database connection failed: {error}")
        raise

    finally:
        engine.dispose()


if __name__ == "__main__":
    test_connection()
