FROM python:3.9-slim

WORKDIR /app

COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/server.py .
COPY config/db_config.json ./config/

CMD ["python", "server.py"]