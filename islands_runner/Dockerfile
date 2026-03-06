FROM python:3.10-slim

WORKDIR /app

COPY islands_runner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY build_islands_from_scores.py .
COPY config/ ./config/

ENTRYPOINT ["python", "build_islands_from_scores.py"]
