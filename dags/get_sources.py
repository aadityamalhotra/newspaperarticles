from newsapi import NewsApiClient
from airflow.models import Variable

def get_all_news_sources():
    # Retrieve your API key from Airflow Variables
    api_key = Variable.get("newsroom_api_key")
    
    # Initialize the client
    newsapi = NewsApiClient(api_key=api_key)

    # Fetch sources
    # Options: category, language, country
    sources_data = newsapi.get_sources(language='en')

    if sources_data['status'] == 'ok':
        sources = sources_data['sources']
        print(f"Successfully retrieved {len(sources)} sources.")
        
        # Displaying the first 5 as an example
        for s in sources[:5]:
            print(f"ID: {s['id']} | Name: {s['name']} | Category: {s['category']}")

        print('|||||||||||||||||||||||||||||||||||||||||')
            
        return [s['id'] for s in sources], [s['name'] for s in sources]
    else:
        print("Error fetching sources.")
        return []

if __name__ == "__main__":
    source_ids, source_names = get_all_news_sources()
    print(source_ids)
    print(source_names)
    print(len(source_names))