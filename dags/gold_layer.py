# necessary imports
import pandas as pd
import spacy
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

# function for staCy implementation
def run_spacy_extraction():

    # establish connections
    conn = BaseHook.get_connection('my_postgres_conn')
    db_uri = conn.get_uri().replace("postgres://", "postgresql://", 1)
    engine = create_engine(db_uri)

    # loading the spaCy model
    nlp = spacy.load("en_core_web_sm")

    # reading data from silver layer table
    query = "SELECT article_id, content FROM news_content"
    df = pd.read_sql(query, engine)

    # list to tore all entities
    all_entities = []

    # processing each article
    for _, row in df.iterrows():
        doc = nlp(row['content'])
        
        for ent in doc.ents:
            # TODO: how to expand on this
            if ent.label_ in ["ORG", "PERSON", "GPE"]:
                all_entities.append({
                    "article_id": row['article_id'],
                    "entity_text": ent.text,
                    "entity_label": ent.label_
                })

    # saving all results
    if all_entities:
        gold_df = pd.DataFrame(all_entities)
        
        # adding the table to database
        with engine.begin() as connection:
            # creating the table if it doesn't exist
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS news_entities_gold (
                    article_id VARCHAR(16),
                    entity_text TEXT,
                    entity_label TEXT
                );
                TRUNCATE TABLE news_entities_gold;
            """))
            
            gold_df.to_sql('news_entities_gold', engine, if_exists='append', index=False)

    print(f"Extraction complete. Found {len(all_entities)} entities.")

with DAG(
    dag_id='news_nlp_gold_v1',
    start_date=datetime(2026, 1, 1),
    schedule=None, # only triggered by silver layer file
    catchup=False
) as dag:

    extract_entities = PythonOperator(
        task_id='spacy_ner_extraction',
        python_callable=run_spacy_extraction
    )