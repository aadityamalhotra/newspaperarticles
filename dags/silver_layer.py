# importing necessary libraries
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime

# function that handles silver layer processing
def prepare_silver_tables():

    # establishing connections
    conn = BaseHook.get_connection('my_postgres_conn')
    db_uri = conn.get_uri().replace("postgres://", "postgresql://", 1)
    engine = create_engine(db_uri)

    # loading the data
    with engine.begin() as connection:
        
        # loading data from news_articles for content table
        content_df = pd.read_sql(text("""
            SELECT article_id, title, url, full_content as content
            FROM news_articles
            WHERE full_content NOT LIKE 'Sorry%' AND length(full_content) >= 300
        """), connection)

        # loading data from news_articles for content table
        brief_df = pd.read_sql(text("""
            SELECT article_id, author, title, published_at as publication_time, content_snippet
            FROM news_articles
            WHERE title NOT SIMILAR TO '[0-9]%'
        """), connection)

        # --- STAGING LOAD ---
        # We load into temporary tables first
        content_df.to_sql('stg_news_content', engine, if_exists='replace', index=False, method='multi', chunksize=100)
        brief_df.to_sql('stg_news_brief', engine, if_exists='replace', index=False, method='multi', chunksize=100)

        # --- ATOMIC SWAP ---
        # We drop the old ones and rename the staging ones to live.
        # This takes milliseconds and ensures zero "downtime" for your tables.
        swap_query = text("""
            DROP TABLE IF EXISTS news_content;
            ALTER TABLE stg_news_content RENAME TO news_content;
            ALTER TABLE news_content ADD PRIMARY KEY (article_id);

            DROP TABLE IF EXISTS news_brief;
            ALTER TABLE stg_news_brief RENAME TO news_brief;
            ALTER TABLE news_brief ADD PRIMARY KEY (article_id);
        """)
        
        connection.execute(swap_query)

    print(f"Silver Tables swapped successfully. Content: {len(content_df)}, Brief: {len(brief_df)}")


# DAG DEFINITION
with DAG(
    dag_id='data_refinery_v1',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    
    refine_data = PythonOperator(
        task_id='create_silver_layer',
        python_callable=prepare_silver_tables
    )