FROM python:3.9-slim

WORKDIR /app

COPY client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Optionally download NLTK data (e.g., punkt) - remove if not needed
RUN python -m nltk.downloader punkt

COPY client/client.py .
RUN mkdir -p ./config
COPY config/db_config.json ./config/
COPY config/models ./config/models/

CMD ["python", "client.py"]