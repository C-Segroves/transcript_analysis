"""
Offline unit tests for client/async_processing_client.py (model-major scoring).

These do NOT require a database or network. They exercise:
  * transcript prep / item building / scoring helpers
  * the DB helper functions (db_get_pending_video_ids, db_get_transcript_text,
    db_load_models, db_save_scores) against an in-memory fake pool
matching the migrated schema (integer ids, model_id).

Run directly:  python test_client_functions.py
Or via:        python run_all_tests.py
"""

import os
import sys

root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root_dir)
sys.path.insert(0, os.path.join(root_dir, "client"))

import test_support as ts
ts.ensure_stubs()  # stub nltk/psutil/psycopg2 only if missing

import async_processing_client as C


def _log():
    import logging
    return logging.getLogger("test_client")


def test_prep_and_items():
    transcript = C.prep_transcript("the quick brown fox jumps over", n_gram_size=4)
    ts.check("prep_transcript left-pads with <s>", transcript[:3] == ["<s>", "<s>", "<s>"])
    ts.check("prep_transcript keeps the words", "quick" in transcript and "fox" in transcript)

    items = C.build_transcript_items(transcript, n_gram_size=4)
    ts.check("build_transcript_items is non-empty", len(items) > 0)
    ts.check("each item is (word, context)", all(len(it) == 2 for it in items))
    ts.check("context length is n_gram_size-1", all(len(ctx) == 3 for _, ctx in items))


def test_scoring():
    model = ts.FakeModel(value=0.5)
    transcript = C.prep_transcript("alpha beta gamma delta epsilon zeta", 4)
    items = C.build_transcript_items(transcript, 4)
    scores = C.score_transcript_items(model, items)
    ts.check("scores produced for every context", len(scores) == sum(1 for _, c in items if c))
    ts.check("scores reflect the model", all(s == 0.5 for s in scores))


def test_db_get_pending_video_ids():
    pool = ts.FakePool(pending_ids=[101, 102, 103])
    ids = C.db_get_pending_video_ids(pool, model_id=7, logger=_log())
    ts.check("returns the pending vid ids", ids == [101, 102, 103])
    ts.check("connection returned to pool", pool.checked_out == 0)


def test_db_get_transcript_text():
    pool = ts.FakePool(transcript_segments=["hello there", "general kenobi"])
    text = C.db_get_transcript_text(pool, vid_id=101, logger=_log())
    ts.check("segments joined in order", text == "hello there general kenobi")


def test_db_load_models():
    pool = ts.FakePool(models={7: ts.FakeModel(0.9), 8: ts.FakeModel(0.1)})
    loaded = C.db_load_models(pool, [7, 8], logger=_log())
    ts.check("both models unpickled", set(loaded.keys()) == {7, 8})
    ts.check("model usable after load", loaded[7].score("w", ["a", "b", "c"]) == 0.9)


def test_db_save_scores():
    pool = ts.FakePool()
    # Capture the batched upsert instead of hitting a real cursor.
    original = C.execute_values
    C.execute_values = ts.make_capturing_execute_values(pool)
    try:
        rows = [(101, 7, [0.1, 0.2]), (102, 7, [0.3])]
        n = C.db_save_scores(pool, rows, logger=_log())
    finally:
        C.execute_values = original
    ts.check("save returns row count", n == 2)
    ts.check("rows captured with (vid_id, model_id, score)", pool.saved == rows)
    ts.check("commit issued", pool.store["commits"] == 1)


def run_all_tests():
    print("\n=== CLIENT FUNCTION TESTS (offline) ===")
    test_prep_and_items()
    test_scoring()
    test_db_get_pending_video_ids()
    test_db_get_transcript_text()
    test_db_load_models()
    test_db_save_scores()
    print("All client function tests passed.")


if __name__ == "__main__":
    run_all_tests()
