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

    # reading data from silver layer
    query = "SELECT article_id, content FROM news_content"
    df = pd.read_sql(query, engine)

    # preparing data for nlp.pipe
    texts = df['content'].tolist()
    article_ids = df['article_id'].tolist()
    all_entities = []
    total_articles = len(texts)

    print(f"Starting NLP extraction for {total_articles} articles using nlp.pipe...")

    # high-speed batch processing
    # as_tuples=True lets us pass (text, context) and get (doc, context) back
    for i, (doc, article_id) in enumerate(nlp.pipe(zip(texts, article_ids), as_tuples=True, batch_size=20)):
        
        for ent in doc.ents:
            if ent.label_ in ["ORG", "PERSON", "GPE"]:
                all_entities.append({
                    "article_id": article_id,
                    "entity_text": ent.text,
                    "entity_label": ent.label_
                })
        
        # Progress Heartbeat: Log every 20 articles
        if (i + 1) % 20 == 0 or (i + 1) == total_articles:
            percent = round(((i + 1) / total_articles) * 100, 2)
            print(f"Progress: {i + 1}/{total_articles} articles processed ({percent}%).")

    # 4. Save to database
    if all_entities:
        gold_df = pd.DataFrame(all_entities)
        with engine.begin() as connection:
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS news_entities_gold (
                    article_id VARCHAR(16),
                    entity_text TEXT,
                    entity_label TEXT
                );
                TRUNCATE TABLE news_entities_gold;
            """))
            # using method='multi' 
            gold_df.to_sql('news_entities_gold', engine, if_exists='append', index=False, method='multi', chunksize=100)

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