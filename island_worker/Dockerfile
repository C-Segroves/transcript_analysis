FROM python:3.10-slim

WORKDIR /app

COPY island_worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Worker + the shared island math + table setup helper
COPY island_worker.py .
COPY build_islands_from_scores.py .
COPY setup_island_tables.py .
COPY config/db_config.json ./config/

# Poll forever by default. Override the command to drain once, seed tasks, tune
# thresholds, etc., e.g.:
#   docker run --rm <image> --refresh --once
#   docker run --rm <image> --threshold 0.6 --min-length 8
ENTRYPOINT ["python", "island_worker.py"]
