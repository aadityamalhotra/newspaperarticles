import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sentence_transformers import SentenceTransformer
import umap
import plotly.express as px
from airflow.sdk import Variable
from airflow.sdk.bases.hook import BaseHook
from newspaper import Article, Config

# 1. DATABASE CONNECTION
api_key = Variable.get("newsroom_api_key")
conn = BaseHook.get_connection(DB_CONN_ID)
DB_CONN_ID = 'my_postgres_conn'
DB_URI = conn.get_uri().replace("postgres://", "postgresql://", 1) # Update with your credentials
engine = create_engine(DB_URI)

def visualize_articles():
    # Load data from the main_table created by your Airflow DAG
    query = "SELECT title, full_content, source_name, url FROM main_table"
    df = pd.read_sql(query, engine)
    
    # Clean data: Combine title and content for better context
    df['text_for_embedding'] = df['title'] + " " + df['full_content'].str.slice(0, 500)
    df = df.dropna(subset=['text_for_embedding'])

    # 2. GENERATE EMBEDDINGS (AI Logic)
    # Using a lightweight, high-performance model
    print("Generating embeddings... this may take a moment.")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = model.encode(df['text_for_embedding'].tolist(), show_progress_bar=True)

    # 3. DIMENSIONALITY REDUCTION (3D)
    # UMAP is excellent for preserving local and global structures in 3D
    reducer = umap.UMAP(n_components=3, n_neighbors=15, min_dist=0.1, random_state=42)
    projections = reducer.fit_transform(embeddings)

    # Add coordinates back to dataframe
    df['x'] = projections[:, 0]
    df['y'] = projections[:, 1]
    df['z'] = projections[:, 2]

    # 4. INTERACTIVE 3D PLOT
    fig = px.scatter_3d(
        df, x='x', y='y', z='z',
        color='source_name',
        hover_name='title',
        hover_data=['source_name'],
        title='Global News Topography (3D Semantic Map)',
        labels={'x': 'Semantic Axis 1', 'y': 'Semantic Axis 2', 'z': 'Semantic Axis 3'},
        opacity=0.7
    )

    # Improve layout
    fig.update_traces(marker=dict(size=4))
    fig.update_layout(margin=dict(l=0, r=0, b=0, t=40))
    
    fig.show()

if __name__ == "__main__":
    visualize_articles()
