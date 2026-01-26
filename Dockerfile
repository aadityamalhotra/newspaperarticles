FROM astrocrpublic.azurecr.io/runtime:3.1-10

FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y build-essential

USER airflow
# Install spaCy and the model weights inside the image
RUN pip install --no-cache-dir spacy==3.7.5
RUN python -m spacy download en_core_web_sm
