FROM python:3.10-slim

WORKDIR /app

COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/server.py .
COPY server/async_processing_server.py .
COPY server/async_server.py .
COPY server/dashboard.py .
COPY maintain_database.py .
COPY packet_handler/ ./packet_handler/
COPY config/db_config.json ./config/
COPY config/YouTube.txt ./config/

# 5000 = task distribution to clients, 8080 = web dashboard
EXPOSE 5000 8080

CMD ["python", "async_processing_server.py"]