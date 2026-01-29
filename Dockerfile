# Remove the constant platform flag to fix the warning
FROM astrocrpublic.azurecr.io/runtime:3.1-10

USER root

# Install system dependencies first
RUN apt-get update && apt-get install -y build-essential gcc && apt-get clean

# Force reinstall the math libraries as ROOT so permissions are set correctly
RUN pip install --no-cache-dir --force-reinstall thinc numpy spacy==3.7.5

# Install the model directly
RUN pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.7.1/en_core_web_sm-3.7.1-py3-none-any.whl

# DO NOT add 'USER airflow' at the end; Astro adds it automatically 
# at the very end of the build process.