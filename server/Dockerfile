FROM python:3.10-slim

WORKDIR /app

COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/server.py .
COPY server/async_processing_server.py .
COPY server/async_server.py .
COPY maintain_database.py . 
COPY packet_handler/ ./packet_handler/
COPY config/db_config.json ./config/
COPY config/YouTube.txt ./config/  

CMD ["python", "async_processing_server.py"]