import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sentence_transformers import SentenceTransformer
import umap
import plotly.express as px
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime
import os

# --- CONFIGURATION ---
DB_CONN_ID = 'my_postgres_conn'

def generate_3d_map(**context):
    # 1. DATABASE CONNECTION (Now works because it's in a DAG)
    conn = BaseHook.get_connection(DB_CONN_ID)
    db_uri = conn.get_uri().replace("postgres://", "postgresql://", 1)
    engine = create_engine(db_uri)
    
    # 2. FETCH DATA
    query = "SELECT title, full_content, source_name FROM article_data"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("No articles found!")
        return

    # 3. PREPROCESS
    df['full_content'] = df['full_content'].fillna('')
    df['text_for_embedding'] = df['title'] + " " + df['full_content'].str.slice(0, 500)
    
    # 4. AI EMBEDDINGS
    print("Encoding articles...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = model.encode(df['text_for_embedding'].tolist(), show_progress_bar=True)

    # 5. UMAP REDUCTION
    print("Reducing to 3D...")
    reducer = umap.UMAP(n_components=3, n_neighbors=15, min_dist=0.1, random_state=42)
    projections = reducer.fit_transform(embeddings)
    df['x'], df['y'], df['z'] = projections[:, 0], projections[:, 1], projections[:, 2]

    # 6. SAVE TO HTML
    # We save to the /dags folder so it appears on your Mac
    fig = px.scatter_3d(
        df, x='x', y='y', z='z',
        color='source_name',
        hover_name='title',
        title='Global News Semantic Map (3D)',
        template='plotly_dark'
    )
    
    dag_folder = os.path.dirname(__file__)  # Gets the directory of the current script
    output_path = os.path.join(dag_folder, "news_map.html")
    fig.write_html(output_path)
    print(f"Success! Map saved to {output_path}")

# --- DAG DEFINITION ---
with DAG(
    dag_id='visualize_news_3d',
    start_date=datetime(2024, 1, 1),
    schedule=None, # Triggered manually or by the other DAG
    catchup=False
) as dag:

    visualize_task = PythonOperator(
        task_id='generate_3d_map',
        python_callable=generate_3d_map
    )