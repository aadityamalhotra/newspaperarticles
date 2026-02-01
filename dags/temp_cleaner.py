import hashlib
from datetime import datetime
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook

# --- CONFIGURATION ---
DB_CONN_ID = 'my_postgres_conn'

def migrate_schema():
    """
    1. Adds source_id column to existing table.
    2. Calculates source_id for all existing rows using MD5(source_name).
    3. Drops unwanted columns (extracted_at, content_snippet).
    """
    conn = BaseHook.get_connection(DB_CONN_ID)
    db_uri = conn.get_uri().replace("postgres://", "postgresql://", 1)
    engine = create_engine(db_uri)

    print("--- STARTING MIGRATION ---")

    with engine.begin() as connection:
        # Step 1: Add the new column (nullable at first)
        print("1. Adding 'source_id' column...")
        connection.execute(text("ALTER TABLE main_table ADD COLUMN IF NOT EXISTS source_id VARCHAR(16);"))

        # Step 2: Fetch unique source names to minimize processing
        print("2. Fetching unique source names...")
        result = connection.execute(text("SELECT DISTINCT source_name FROM main_table WHERE source_id IS NULL;"))
        unique_sources = [row[0] for row in result]
        
        print(f"   -> Found {len(unique_sources)} sources needing IDs.")

        # Step 3: Calculate IDs and Update in Batch
        # We do this in Python to ensure the hashing algorithm is IDENTICAL to your ingestion script
        for source in unique_sources:
            safe_name = str(source).strip().lower() if source else "unknown"
            # EXACT SAME ENCRYPTION AS FILE 1
            new_id = hashlib.md5(safe_name.encode()).hexdigest()[:16]
            
            # Handle Single Quotes in SQL by doubling them (e.g. "People's Daily")
            sql_safe_name = str(source).replace("'", "''")
            
            update_query = text(f"""
                UPDATE main_table 
                SET source_id = '{new_id}' 
                WHERE source_name = '{sql_safe_name}';
            """)
            connection.execute(update_query)
        
        print("   -> IDs populated.")

        # Step 4: Drop unwanted columns
        # We use IF EXISTS so the script doesn't crash if you run it twice
        print("3. Dropping obsolete columns...")
        connection.execute(text("ALTER TABLE main_table DROP COLUMN IF EXISTS content_snippet;"))
        connection.execute(text("ALTER TABLE main_table DROP COLUMN IF EXISTS extracted_at;"))

    print("--- MIGRATION COMPLETE: Schema is now clean. ---")

with DAG(
    dag_id='one_time_migration_script',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    run_migration = PythonOperator(
        task_id='fix_database_schema',
        python_callable=migrate_schema
    )