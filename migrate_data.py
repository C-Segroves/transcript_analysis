# migrate_data.py (WITH PROGRESS PRINTS)

import psycopg2
import json
import os
from datetime import datetime

def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

def load_db_params():
    return load_db_config()

def safe_json_dump(obj):
    """Safely serialize object to JSON string, handling datetime and other types"""
    if obj is None:
        return None
    return json.dumps(obj, default=str)

def migrate_data():
    db_params = load_db_params()
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    try:
        print("Starting data migration...")
        print("=" * 60)

        # Step 1: Migrate MODEL_TABLE
        print(f"Step 1: Migrating MODEL_TABLE...{datetime.now()}")
        model_map = {}
        cursor.execute("SELECT model_key, model_data, insert_at FROM model_table")
        model_count = 0
        for old_key, model_data, insert_at in cursor.fetchall():
            cursor.execute("""
                INSERT INTO new_model_table (external_key, model_data, insert_at)
                VALUES (%s, %s, %s) RETURNING id
            """, (old_key, model_data, insert_at))
            model_map[old_key] = cursor.fetchone()[0]
            model_count += 1
        print(f"   → Migrated {model_count} models")

        # Step 2: Migrate CHANNEL_TABLE
        print(f"Step 2: Migrating CHANNEL_TABLE...{datetime.now()}")
        channel_map = {}
        cursor.execute("SELECT channel_id, channel_handle, channel_snippet, insert_at FROM channel_table")
        channel_count = 0
        for old_id, handle, snippet, insert_at in cursor.fetchall():
            json_snippet = safe_json_dump(snippet)
            cursor.execute("""
                INSERT INTO new_channel_table (external_id, handle, snippet, insert_at)
                VALUES (%s, %s, %s, %s) RETURNING id
            """, (old_id, handle, json_snippet, insert_at))
            channel_map[old_id] = cursor.fetchone()[0]
            channel_count += 1
        print(f"   → Migrated {channel_count} channels")

        # Step 3: Migrate VID_TABLE
        print(f"Step 3: Migrating VID_TABLE...{datetime.now()}")
        vid_map = {}
        cursor.execute("SELECT channel_id, vid_id, insert_at FROM vid_table")
        vid_count = 0
        skipped_vids = 0
        for old_channel_id, old_vid_id, insert_at in cursor.fetchall():
            new_channel_id = channel_map.get(old_channel_id)
            if not new_channel_id:
                print(f"   ⚠️  Skipping vid {old_vid_id}: missing channel {old_channel_id}")
                skipped_vids += 1
                continue
            cursor.execute("""
                INSERT INTO new_vid_table (channel_id, external_id, insert_at)
                VALUES (%s, %s, %s) RETURNING id
            """, (new_channel_id, old_vid_id, insert_at))
            vid_map[old_vid_id] = cursor.fetchone()[0]
            vid_count += 1
        print(f"   → Migrated {vid_count} videos ({skipped_vids} skipped)")

        # Step 4: Migrate VID_DATA_TABLE
        print(f"Step 4: Migrating VID_DATA_TABLE...{datetime.now()}")
        cursor.execute("""
            SELECT vid_id, title, description, publishtime, snippet, insert_at
            FROM vid_data_table
        """)
        vid_data_count = 0
        for old_vid_id, title, desc, publish_time, snippet, insert_at in cursor.fetchall():
            new_vid_id = vid_map.get(old_vid_id)
            if not new_vid_id:
                continue
            json_snippet = safe_json_dump(snippet)
            cursor.execute("""
                INSERT INTO new_vid_data_table (vid_id, title, description, publish_time, snippet, insert_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (new_vid_id, title, desc, publish_time, json_snippet, insert_at))
            vid_data_count += 1
        print(f"   → Migrated {vid_data_count} video metadata records")

        # Step 5: Migrate VID_TRANSCRIPT_TABLE
        print(f"Step 5: Migrating VID_TRANSCRIPT_TABLE...{datetime.now()}")
        cursor.execute("""
            SELECT vid_id, text, start, duration, text_formatted, word_count, cum_word_count, insert_at
            FROM vid_transcript_table
        """)
        transcript_count = 0
        for old_vid_id, text, start, duration, text_formatted, word_count, cum_word_count, insert_at in cursor.fetchall():
            new_vid_id = vid_map.get(old_vid_id)
            if not new_vid_id:
                continue
            cursor.execute("""
                INSERT INTO new_vid_transcript_table (vid_id, text, start, duration, text_formatted, word_count, cum_word_count, insert_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (new_vid_id, text, start, duration, text_formatted, word_count, cum_word_count, insert_at))
            transcript_count += 1
        print(f"   → Migrated {transcript_count} transcript segments")

        # Step 6: Migrate THRESHOLD_TABLE
        #print("Step 6: Migrating THRESHOLD_TABLE...")
        #threshold_map = {}
        #cursor.execute("SELECT threshold_id, threshold_value, description, created_at FROM threshold_table")
        #threshold_count = 0
        #for old_id, value, desc, created_at in cursor.fetchall():
        #    cursor.execute("""
        #        INSERT INTO new_threshold_table (threshold_value, description, created_at)
        #        VALUES (%s, %s, %s) RETURNING id
        #    """, (value, desc, created_at))
        #    threshold_map[old_id] = cursor.fetchone()[0]
        #    threshold_count += 1
        #print(f"   → Migrated {threshold_count} thresholds")

        # ------------------------------------------------------------------
        # Step 7 – VID_SCORE_TABLE (Memory-safe, batched)
        # ------------------------------------------------------------------
        print(f"Step 7: Migrating VID_SCORE_TABLE (batched, memory-safe)...{datetime.now()}")

        BATCH_SIZE = 10_000
        score_count = 0
        skipped_count = 0

        # Use a named, server-side cursor
        with connection.cursor(name='vid_score_cursor') as server_cursor:
            server_cursor.itersize = BATCH_SIZE
            server_cursor.execute("""
                SELECT vid_id, model_key, score, insert_at 
                FROM vid_score_table 
                ORDER BY vid_id, model_key
            """)

            batch = []
            while True:
                rows = server_cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                for old_vid_id, old_model_key, score, insert_at in rows:
                    new_vid_id = vid_map.get(old_vid_id)
                    new_model_id = model_map.get(old_model_key)
                    if not new_vid_id or not new_model_id:
                        skipped_count += 1
                        continue
                    batch.append((new_vid_id, new_model_id, score, insert_at))
                    score_count += 1

                # Insert batch
                if batch:
                    cursor.executemany("""
                        INSERT INTO new_vid_score_table (vid_id, model_id, score, insert_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (vid_id, model_id) DO NOTHING
                    """, batch)
                    batch.clear()

                print(f"   → Processed {score_count:,} score rows so far...", end='\r')

        print(f"\n   → Migrated {score_count:,} score records ({skipped_count:,} skipped)")

        # Step 8: Migrate VID_SCORE_ANALYTICS_TABLE
        print(f"Step 8: Migrating VID_SCORE_ANALYTICS_TABLE...{datetime.now()}")
        cursor.execute("SELECT vid_id, model_key, machine_name, time_taken, insert_at FROM vid_score_analytics_table")
        analytics_count = 0
        for old_vid_id, old_model_key, machine, time_taken, insert_at in cursor.fetchall():
            new_vid_id = vid_map.get(old_vid_id)
            new_model_id = model_map.get(old_model_key)
            if not new_vid_id or not new_model_id:
                continue
            cursor.execute("""
                INSERT INTO new_vid_score_analytics_table (vid_id, model_id, machine_name, time_taken, insert_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (new_vid_id, new_model_id, machine, time_taken, insert_at))
            analytics_count += 1
        print(f"   → Migrated {analytics_count} analytics records")

        # Step 9: Migrate ASSIGNED_TASKS
        print(f"Step 9: Migrating ASSIGNED_TASKS...{datetime.now()}")
        cursor.execute("SELECT vid_id, model_key FROM assigned_tasks")
        task_count = 0
        for old_vid_id, old_model_key in cursor.fetchall():
            new_vid_id = vid_map.get(old_vid_id)
            new_model_id = model_map.get(old_model_key)
            if not new_vid_id or not new_model_id:
                continue
            cursor.execute("""
                INSERT INTO new_assigned_tasks (vid_id, model_id)
                VALUES (%s, %s)
            """, (new_vid_id, new_model_id))
            task_count += 1
        print(f"   → Migrated {task_count} assigned tasks")

        # Step 10: Migrate VID_MODEL_STATE
        print(f"Step 10: Migrating VID_MODEL_STATE...{datetime.now()}")
        cursor.execute("SELECT vid_id, model_key, state FROM vid_model_state")
        state_count = 0
        for old_vid_id, old_model_key, state in cursor.fetchall():
            new_vid_id = vid_map.get(old_vid_id)
            new_model_id = model_map.get(old_model_key)
            if not new_vid_id or not new_model_id:
                continue
            cursor.execute("""
                INSERT INTO new_vid_model_state (vid_id, model_id, state)
                VALUES (%s, %s, %s)
            """, (new_vid_id, new_model_id, state))
            state_count += 1
        print(f"   → Migrated {state_count} model states")

        ## Step 11: Migrate SCORE_ISLAND_TABLE
        #print("Step 11: Migrating SCORE_ISLAND_TABLE...")
        #cursor.execute("""
        #    SELECT vid_id, model_key, threshold_id, start_index, end_index, insert_at
        #    FROM score_island_table
        #""")
        #island_count = 0
        #for old_vid_id, old_model_key, old_thr_id, start_idx, end_idx, insert_at in cursor.fetchall():
        #    new_vid_id = vid_map.get(old_vid_id)
        #    new_model_id = model_map.get(old_model_key)
        #    new_thr_id = threshold_map.get(old_thr_id)
        #    if not all([new_vid_id, new_model_id, new_thr_id]):
        #        continue
        #    cursor.execute("""
        #        INSERT INTO new_score_island_table (vid_id, model_id, threshold_id, start_index, end_index, insert_at)
        #        VALUES (%s, %s, %s, %s, %s, %s)
        ##    """, (new_vid_id, new_model_id, new_thr_id, start_idx, end_idx, insert_at))
        # #   island_count += 1
        #print(f"   → Migrated {island_count} score islands")

        connection.commit()
        print("=" * 60)
        print("DATA MIGRATION COMPLETED SUCCESSFULLY!")
        print("=" * 60)

    except Exception as error:
        print(f"ERROR during migration: {error}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    migrate_data()