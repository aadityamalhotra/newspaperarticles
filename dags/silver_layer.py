# importing necessary libraries
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# function that handles silver layer processing
def prepare_silver_tables():

    # establishing connections
    conn = BaseHook.get_connection('my_postgres_conn')
    db_uri = conn.get_uri().replace("postgres://", "postgresql://", 1)
    engine = create_engine(db_uri)

    # loading the data
    with engine.begin() as connection:
        
        # loading data from articles for content table
        content_df = pd.read_sql(text("""
            SELECT article_id, title, url, full_content as content
            FROM articles
            WHERE full_content NOT LIKE 'Sorry%' AND length(full_content) >= 300
        """), connection)

        # loading data from articles for content table
        brief_df = pd.read_sql(text("""
            SELECT article_id, author, title, published_at as publication_time, content_snippet
            FROM articles
            WHERE title NOT SIMILAR TO '[0-9]%'
        """), connection)

        # We load into temporary staging tables first
        content_df.to_sql('stg_news_content', engine, if_exists='replace', index=False, method='multi', chunksize=100)
        brief_df.to_sql('stg_news_brief', engine, if_exists='replace', index=False, method='multi', chunksize=100)


        # We drop the old ones and rename the staging ones to live.
        swap_query = text("""
            DROP TABLE IF EXISTS news_content;
            ALTER TABLE stg_news_content RENAME TO news_content;
            ALTER TABLE news_content ADD PRIMARY KEY (article_id);

            DROP TABLE IF EXISTS news_brief;
            ALTER TABLE stg_news_brief RENAME TO news_brief;
            ALTER TABLE news_brief ADD PRIMARY KEY (article_id);
        """)
        
        # execute the query
        connection.execute(swap_query)

    print(f"Silver Tables swapped successfully. Content: {len(content_df)}, Brief: {len(brief_df)}")


# DAG DEFINITION
with DAG(
    dag_id='data_refinery_v1',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    refine_data = PythonOperator(
        task_id='create_silver_layer',
        python_callable=prepare_silver_tables
    )

    # call the golden layer dag
    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_nlp',
        trigger_dag_id='news_nlp_gold_v1', # This must match the Gold DAG ID
        wait_for_completion=False
    )

    refine_data >> trigger_gold