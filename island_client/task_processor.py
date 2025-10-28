import time


async def process_task(transcript_items, model_key,model, vid_id, n_gram_size, logger):
    try:
        start_time = time.time()

        # Ensure the model is loaded
        if not model:
            raise ValueError(f"Model {model_key} not found in model_dict.")

        # Process transcript items and calculate scores
        score = [model.score(item, ngram) for item, ngram in transcript_items if ngram]
        time_taken = time.time() - start_time

        return {'model_key': model_key, 'score': score, 'time_taken': time_taken}
    except Exception as e:
        logger.error(f"Processing error for VID_ID {vid_id}, MODEL_KEY {model_key}: {e}")
        return {'model_key': model_key, 'score': [], 'time_taken': 0}