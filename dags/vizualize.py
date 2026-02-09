import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import plotly.express as px

# 1. Load the data
file_path = '/Users/aadityamalhotra/general_post_moltbook.csv'

try:
    df = pd.read_csv(file_path)
except FileNotFoundError:
    print(f"File {file_path} not found. Generating sample data...")
    data = {
        'post_id': range(1, 11),
        'title': ['Python Loop Tutorial', 'Deep Dive into AI', 'Pizza Recipe', 'How to Code', 'The Landscape of AI', 'Pasta Tips', 'JS Tutorial', 'AI Future Slop', 'Cookie Recipe', 'Coding help'],
        'content': ['Use for loops...', 'AI is evolving fast...', 'Add cheese...', 'Coding is fun...', 'In today’s evolving landscape...', 'Boil water...', 'Let vs Var...', 'Deep testament to AI...', 'Bake at 350...', 'Help with syntax...'],
        'upvotes': np.random.randint(1, 100, 10),
        'comment_count': np.random.randint(1, 20, 10),
        'poster_name': ['User'+str(i) for i in range(10)]
    }
    df = pd.DataFrame(data)

# 2. Vectorize Text
df['combined_text'] = df['title'].fillna('') + " " + df['content'].fillna('')
vectorizer = TfidfVectorizer(stop_words='english', max_features=1000)
tfidf_matrix = vectorizer.fit_transform(df['combined_text'])

# 3. Clustering (K-Means)
num_clusters = 5
kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
df['cluster'] = kmeans.fit_predict(tfidf_matrix).astype(str) # Cast to string for discrete colors

# 4. Dimensionality Reduction (PCA to 3D)
pca = PCA(n_components=3)
components = pca.fit_transform(tfidf_matrix.toarray())
df['x'], df['y'], df['z'] = components[:, 0], components[:, 1], components[:, 2]

# 5. Identify Cluster "Meaning"
print("\n" + "="*30)
print("CLUSTER KEYWORD INTERPRETATION")
print("="*30)
order_centroids = kmeans.cluster_centers_.argsort()[:, ::-1]
terms = vectorizer.get_feature_names_out()

for i in range(num_clusters):
    top_words = [terms[ind] for ind in order_centroids[i, :8]]
    print(f"Cluster {i}: {' | '.join(top_words)}")
print("="*30 + "\n")

# 6. Visualize with Massive Points and 0.7 Alpha
fig = px.scatter_3d(
    df, x='x', y='y', z='z',
    color='cluster',
    size='comment_count', 
    size_max=70,               # Increased by ~7x (original was 10-15)
    hover_data=['title', 'poster_name'],
    title='3D Semantic Map: Cluster View',
    template='plotly_dark',
    color_discrete_sequence=px.colors.qualitative.Prism
)

# Set the alpha (opacity) to 0.7
fig.update_traces(marker=dict(opacity=0.7, line=dict(width=0)))

# Improve the layout for better visibility of the large points
fig.update_layout(margin=dict(l=0, r=0, b=0, t=40))

fig.show()