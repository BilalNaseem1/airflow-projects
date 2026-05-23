"""
Connection tests for the weather pipeline.
Run inside the scheduler container:
    python tests/test_connections.py
"""
from airflow.providers.postgres.hooks.postgres import PostgresHook


def test_postgres_connection():
    print("\n--- Testing Postgres Connection ---")
    try:
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # test 1 — basic connectivity
        cursor.execute("SELECT current_database(), current_user;")
        db, user = cursor.fetchone()
        print(f"✓ Connected to database : {db}")
        print(f"✓ Authenticated as user : {user}")

        # test 2 — correct database
        assert db == "weather_db", f"✗ Expected weather_db, got {db}"
        print("✓ Correct database confirmed")

        # test 3 — can create tables (write permission)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS _connection_test (
                id SERIAL PRIMARY KEY,
                tested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        print("✓ Write permission confirmed")

        # test 4 — clean up test table
        cursor.execute("DROP TABLE _connection_test;")
        conn.commit()
        print("✓ Cleanup successful")

        cursor.close()
        conn.close()
        print("\n✓ All connection tests passed\n")

    except Exception as e:
        print(f"\n✗ Connection test failed: {e}\n")
        raise


if __name__ == "__main__":
    test_postgres_connection()