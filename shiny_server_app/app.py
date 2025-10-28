from shiny import App, ui, reactive, render
import psycopg2
import pandas as pd
import json
import urllib.request
from datetime import datetime
import logging
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s [%(levelname)s] %(message)s",  # Log format
    handlers=[
        logging.StreamHandler()  # Log to stdout (Docker captures stdout)
    ]
)

logger = logging.getLogger(__name__)

# Database connection configuration
def load_config():
    with open('db_config.json', 'r') as f:
        return json.load(f)

def get_db_connection():
    config = load_config()
    return psycopg2.connect(
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"]
    )

def get_api_key():
    with open('YouTube.txt', 'r') as f:
        return f.read().strip()
    
api_key=get_api_key()

logger.info(f"API Key: {api_key}")

# Function to insert a new channel into the database
def insert_new_channel(channel_handle, channel_id, channel_snippet):
    insert_query = """
    INSERT INTO CHANNEL_TABLE (CHANNEL_HANDLE, CHANNEL_ID, CHANNEL_SNIPPET, INSERT_AT)
    VALUES (%s, %s, %s, %s)
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(insert_query, (
                channel_handle,
                channel_id,
                json.dumps(channel_snippet),
                datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            ))

# Function to fetch channel details from the YouTube API
def fetch_channel_details(channel_handle, api_key):
    try:
        logger.info(f"CHANNEL_HANDLE: {channel_handle}")
        # Get channel ID
        url = f'https://www.googleapis.com/youtube/v3/channels?key={api_key}&forHandle={channel_handle}'
        with urllib.request.urlopen(url, timeout=1) as response:
            resp = json.load(response)
            #logger.info(f"Response: {resp}")
            #logger.info(f"Response: {resp['items']}")
            #logger.info(f"Response: {resp['items']}")
            channel_id = resp['items'][0]['id']
        
        logger.info(f"CHANNEL_ID: {channel_id}")

        # Get channel metadata (snippet)
        channel_url = f'https://www.googleapis.com/youtube/v3/channels?key={api_key}&id={channel_id}&part=snippet'
        with urllib.request.urlopen(channel_url, timeout=1) as response:
            resp2 = json.load(response)
            #logger.info(f"Response: {resp2}")
            snippet = resp2['items'][0]['snippet']
        
        logger.info(f"Snippet: {snippet}")

        return channel_id, snippet
    except Exception as e:
        logger.info(f"Error fetching channel details: {e}")
        return None, None

# Define the UI
app_ui = ui.page_fluid(
    ui.h2("Database Status"),
    ui.output_table("results_table"),
    ui.input_action_button("refresh_btn", "Refresh Data"),
    ui.h2("Add New Channel"),
    ui.input_text("channel_handle_input", "Channel Handle"),
    ui.input_action_button("add_channel_btn", "Add Channel"),
    ui.output_text("add_channel_status"),
    ui.h2("Maintenance"),
    ui.input_action_button("run_maintenance_btn", "Run Maintenance"),
    ui.output_text("maintenance_status")
)

# Define the server logic
def server(input, output, session):
    # Reactive value to store the query results
    data = reactive.Value(None)

    # Reactive value to store the status of adding a channel
    add_channel_status = reactive.Value("")

    @reactive.Effect
    @reactive.event(input.refresh_btn)
    def refresh_data():
        conn = get_db_connection()
        try:
            query = """
            SELECT 
                ROUND(
                    (SUM(CASE WHEN state = 'complete' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)),
                    4
                ) AS percent_complete,
                COUNT(*) AS total_entries,
                SUM(CASE WHEN state = 'complete' THEN 1 ELSE 0 END) AS total_complete,
                SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END) AS total_pending,
                SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) AS total_null
            FROM vid_model_state;
            """
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()  # Fetch all rows
                column_names = [desc[0] for desc in cursor.description]  # Get column names
                df = pd.DataFrame(results, columns=column_names)  # Convert to DataFrame
                data.set(df)  # Store the DataFrame in the reactive value
        finally:
            conn.close()

    # Render the table output
    @output
    @render.table
    def results_table():
        df = data.get()
        if df is None or df.empty:
            return pd.DataFrame([["No data available"]], columns=["Message"])  # Return placeholder
        return df

    # Handle adding a new channel
    @reactive.Effect
    @reactive.event(input.add_channel_btn)
    def add_channel():
        channel_handle = input.channel_handle_input()
        if not channel_handle:
            add_channel_status.set("Please enter a channel handle.")
            return

        #api_key = load_config()["api_key"]  # Load the API key from the config
        channel_id, snippet = fetch_channel_details(channel_handle, api_key)
        logger.info(f"Channel ID to display: {channel_id}")
        if channel_id and snippet:
            try:
                insert_new_channel(channel_handle, channel_id, snippet)
                add_channel_status.set(f"Channel '{channel_handle}' added successfully.")
            except Exception as e:
                add_channel_status.set(f"Error adding channel: {e}")
        else:
            add_channel_status.set(f"Failed to fetch details for channel '{channel_handle}'.")

    # Render the status of adding a channel
    @output
    @render.text
    def add_channel_status_output():
        return add_channel_status.get()

# Create the app
app = App(app_ui, server)