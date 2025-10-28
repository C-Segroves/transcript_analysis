FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the application files
COPY shiny_server_app/app.py .
COPY shiny_server_app/requirements.txt .
COPY config/db_config.json .
COPY config/YouTube.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port Shiny will run on
EXPOSE 8000

# Run the Shiny app
CMD ["shiny", "run", "--host", "0.0.0.0", "--port", "8000", "app.py"]