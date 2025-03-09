import subprocess
import json
import os
import tempfile
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load database configuration from file
def load_db_config():
    try:
        with open('config/db_config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load db_config.json: {e}")
        raise

def dump_production_database(db_config, dump_file):
    """Dump the production database to a file."""
    try:
        cmd = [
            "pg_dump",
            "-h", db_config['host'],
            "-p", str(db_config['port']),
            "-U", db_config['user'],
            "-F", "p",  # Plain SQL format
            "-f", dump_file,
            db_config['dbname']
        ]
        env = os.environ.copy()
        env["PGPASSWORD"] = db_config['password']
        subprocess.run(cmd, check=True, env=env, capture_output=True, text=True)
        logger.info(f"Dumped production database to {dump_file}")
    except subprocess.CalledProcessError as e:
        logger.error(f"pg_dump failed: {e.stderr}")
        raise

def filter_dump_for_channel(dump_file, channel_id, filtered_file):
    """Filter the dump to include only data for the specified channel."""
    try:
        with open(dump_file, 'r', encoding='utf-8') as f_in, open(filtered_file, 'w', encoding='utf-8') as f_out:
            vid_ids = set()
            keep_section = False
            current_table = None

            for line in f_in:
                # Detect table sections
                if line.startswith('COPY public.') and 'FROM stdin;' in line:
                    current_table = line.split()[1].split('.')[1]
                    keep_section = True
                    f_out.write(line)

                # End of COPY section
                elif line.strip() == '\\.':
                    if keep_section:
                        f_out.write(line)
                    keep_section = False
                    current_table = None

                # Filter data rows
                elif keep_section and current_table:
                    if current_table == 'channel_table':
                        if f" {channel_id}\t" in line:  # CHANNEL_ID is typically second column
                            f_out.write(line)
                    elif current_table == 'vid_table':
                        if f" {channel_id}\t" in line:  # CHANNEL_ID is first column
                            vid_id = line.split('\t')[1]  # VID_ID is second column
                            vid_ids.add(vid_id)
                            f_out.write(line)
                    elif current_table in ('vid_data_table', 'vid_transcript_table', 'vid_model_state', 
                                         'vid_score_table', 'vid_score_analytics_table'):
                        vid_id = line.split('\t')[0]  # VID_ID is first column
                        if vid_id in vid_ids:
                            f_out.write(line)
                    elif current_table == 'model_table':
                        f_out.write(line)  # Keep all models
                    else:
                        f_out.write(line)  # Schema definitions, etc.

                # Schema and other non-data lines
                else:
                    f_out.write(line)

        logger.info(f"Filtered dump for CHANNEL_ID {channel_id} to {filtered_file}")
    except Exception as e:
        logger.error(f"Error filtering dump: {e}")
        raise

def create_and_restore_dev_database(db_config, filtered_file):
    """Create dev_database and restore the filtered dump."""
    dev_db_name = "dev_database"
    try:
        # Create dev_database
        cmd_create = [
            "createdb",
            "-h", db_config['host'],
            "-p", str(db_config['port']),
            "-U", db_config['user'],
            dev_db_name
        ]
        env = os.environ.copy()
        env["PGPASSWORD"] = db_config['password']
        subprocess.run(cmd_create, check=True, env=env, capture_output=True, text=True)
        logger.info(f"Created dev_database: {dev_db_name}")

        # Restore filtered dump
        cmd_restore = [
            "psql",
            "-h", db_config['host'],
            "-p", str(db_config['port']),
            "-U", db_config['user'],
            "-d", dev_db_name,
            "-f", filtered_file
        ]
        subprocess.run(cmd_restore, check=True, env=env, capture_output=True, text=True)
        logger.info(f"Restored filtered dump into {dev_db_name}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Database creation/restore failed: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during restore: {e}")
        raise

def create_dev_database(channel_id):
    db_config = load_db_config()
    
    # Use temporary files for dump and filtered output
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sql') as dump_tmp, \
         tempfile.NamedTemporaryFile(delete=False, suffix='.sql') as filtered_tmp:
        dump_file = dump_tmp.name
        filtered_file = filtered_tmp.name
        
        try:
            # Step 1: Dump production database
            dump_production_database(db_config, dump_file)
            
            # Step 2: Filter for one channel
            filter_dump_for_channel(dump_file, channel_id, filtered_file)
            
            # Step 3: Create and restore dev_database
            create_and_restore_dev_database(db_config, filtered_file)
            
        finally:
            # Clean up temporary files
            os.remove(dump_file)
            os.remove(filtered_file)
            logger.info("Cleaned up temporary files")

if __name__ == "__main__":
    # Example CHANNEL_ID from your logs
    channel_id = "UCnTDQ1ggnV_wdm6JsHVBAaQ"
    create_dev_database(channel_id)