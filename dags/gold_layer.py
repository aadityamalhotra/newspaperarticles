# necessary imports
import pandas as pd
import spacy
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

def run_spacy_extraction():
    # --- 1. DATABASE CONNECTION ---
    # Retrieve connection details from Airflow and prepare the SQLAlchemy engine
    conn = BaseHook.get_connection('my_postgres_conn')
    db_uri = conn.get_uri().replace("postgres://", "postgresql://", 1)
    engine = create_engine(db_uri)

    # --- 2. MODEL LOADING ---
    # We attempt to load the Medium model first (better vectors), then fallback to Small.
    # We do NOT use 'disable' here because components like 'tok2vec' are 
    # required for the Tagger and Parser to function correctly.
    try:
        nlp = spacy.load("en_core_web_md")
        print("Successfully loaded en_core_web_md")
    except OSError:
        print("Medium model not found, falling back to en_core_web_sm")
        import en_core_web_sm
        nlp = en_core_web_sm.load()

    print(f"Active Pipeline Components: {nlp.pipe_names}")

    # --- 3. DATA ACQUISITION ---
    query = "SELECT article_id, content FROM news_content"
    df = pd.read_sql(query, engine)
    
    # Pre-cleaning: Ensure all content is a string and handle potential NaNs
    # Passing non-string objects to nlp.pipe can cause silent failures or bad POS tags.
    texts = df['content'].fillna('').astype(str).tolist()
    article_ids = df['article_id'].tolist()
    
    all_entities = []
    total_articles = len(texts)

    # TODO: remove
    debug_limit = 10  # Limit debug prints for null cases
    null_debug_count = 0

    print(f"Starting NLP extraction for {total_articles} articles...")

    # --- 4. NLP PROCESSING LOOP ---
    # Using nlp.pipe with as_tuples=True allows us to pass the article_id alongside the text
    # for easy mapping back to our records.
    for i, (doc, article_id) in enumerate(nlp.pipe(
        zip(texts, article_ids), 
        as_tuples=True, 
        batch_size=20
    )):
        
        # Periodic Debugging: Print first 10 tokens of the first few articles
        # This confirms that 'Major' is an ADJ and 'will' is an AUX (not all NOUNs)
        if i < 3:
            print(f"\n--- POS Verification for Article {article_id} ---")
            print([(t.text, t.pos_) for t in doc[:10]])

        for ent in doc.ents:
            if ent.label_ in ["ORG", "PERSON", "GPE"]:
                descriptors = []
                
                # --- 1. GRAMMATICAL SEARCH (High Precision) ---
                # Check direct children and the head (same as before)
                for token in ent:
                    for child in token.children:
                        if child.pos_ == "ADJ":
                            descriptors.append(child.text.lower())
                
                root_verb = ent.root.head
                if root_verb.pos_ in ["VERB", "AUX", "NOUN"]: # Added NOUN for cases like 'northern towns'
                    for child in root_verb.children:
                        if child.pos_ == "ADJ":
                            descriptors.append(child.text.lower())

                # --- 2. WINDOW SEARCH (Higher Recall) ---
                # If we still have nothing, look at words immediately surrounding the entity
                if not descriptors:
                    # Get indices for 3 words before and 3 words after
                    start_idx = max(0, ent.start - 3)
                    end_idx = min(len(doc), ent.end + 3)
                    
                    for i in range(start_idx, end_idx):
                        # Don't capture words that are part of the entity itself
                        if i < ent.start or i >= ent.end:
                            token = doc[i]
                            if token.pos_ == "ADJ":
                                descriptors.append(token.text.lower())

                # Clean up: Remove duplicates and common unhelpful words
                unique_descriptors = sorted(list(set(descriptors)))
                
                all_entities.append({
                    "article_id": article_id,
                    "entity_text": ent.text,
                    "entity_label": ent.label_,
                    "associated_words": ", ".join(unique_descriptors) if unique_descriptors else None
                })
        
        # Heartbeat log every 50 articles
        if (i + 1) % 50 == 0 or (i + 1) == total_articles:
            print(f"Progress: {i + 1}/{total_articles} articles processed.")

    # --- 5. DATABASE UPLOAD ---
    if not all_entities:
        print("No entities found. Skipping database upload.")
        return

    gold_df = pd.DataFrame(all_entities)
    print(f"Extraction complete. Uploading {len(gold_df)} rows to news_entities_gold...")
    
    try:
        with engine.connect() as connection:
            with connection.begin():
                # We DROP the table to ensure the schema is fresh (associated_words column)
                connection.execute(text("DROP TABLE IF EXISTS news_entities_gold CASCADE;"))
                
                connection.execute(text("""
                    CREATE TABLE news_entities_gold (
                        article_id VARCHAR(50),
                        entity_text TEXT,
                        entity_label VARCHAR(20),
                        associated_words TEXT
                    );
                """))
                
                gold_df.to_sql(
                    'news_entities_gold', 
                    connection, 
                    if_exists='append', 
                    index=False, 
                    method='multi', 
                    chunksize=500
                )
        print("Database upload successful!")
    except Exception as e:
        print(f"Database Error: {e}")
        raise

    print(f"Final Count: Found {len(all_entities)} entities.")



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