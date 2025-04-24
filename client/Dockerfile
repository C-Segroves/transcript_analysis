FROM python:3.10-slim

WORKDIR /app

COPY client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m nltk.downloader punkt

COPY client/client.py .
COPY client/async_client.py .
COPY client/async_processing_client.py .
COPY client/task_processor.py .
COPY packet_handler/ ./packet_handler/
COPY config/db_config.json ./config/

CMD ["python", "async_processing_client.py"]