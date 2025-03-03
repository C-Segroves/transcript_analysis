FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Optionally download NLTK data (e.g., punkt) - remove if not needed
RUN python -m nltk.downloader punkt

# Copy the client.py file
COPY client.py .

# Copy the db_config.json file into config directory
COPY ../config/db_config.json ./config/

# Copy the entire models directory from ../config/models
COPY ../config/models ./config/models/

CMD ["python", "client.py"]