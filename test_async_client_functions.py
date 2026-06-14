"""
Offline unit tests for the async, model-major flow of the scoring client
(client/async_processing_client.py): the ProcessingClient request/score/report
loop and its RAM-aware LRU model cache.

No database or network required. asyncio.to_thread is replaced with an inline
shim so the tests don't depend on a thread pool, and the "server" is simulated
by replying to each work_request the client sends.

Run directly:  python test_async_client_functions.py
Or via:        python run_all_tests.py
"""

import os
import sys
import asyncio
import logging

os.environ.setdefault("NO_WORK_POLL_SECONDS", "0")  # don't really sleep on no_work
os.environ.setdefault("MODEL_CACHE_SIZE", "2")      # small cache so we can test eviction

root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root_dir)
sys.path.insert(0, os.path.join(root_dir, "client"))

import test_support as ts
ts.ensure_stubs()

import async_processing_client as C

# Set tunables directly on the module so the tests are independent of import
# order (env vars only take effect if read before the module was first imported,
# which isn't guaranteed when another suite imports it first).
C.NO_WORK_POLL_SECONDS = 0  # don't really sleep when the server says no_work

# Run to_thread work inline (no executor needed in the test environment).
async def _inline(fn, *a, **k):
    return fn(*a, **k)
asyncio.to_thread = _inline

_LOG = logging.getLogger("test_async_client")


def _make_client(pool):
    client = C.ProcessingClient("h", "box", 5000, _LOG, {}, pool)
    return client


def test_ensure_model_lru_eviction():
    async def go():
        saved_cap = C.MODEL_CACHE_SIZE
        C.MODEL_CACHE_SIZE = 2  # force a small cache so eviction is observable
        try:
            pool = ts.FakePool(models={1: ts.FakeModel(), 2: ts.FakeModel(), 3: ts.FakeModel()})
            c = _make_client(pool)
            await c.ensure_model(1)
            await c.ensure_model(2)
            ts.check("two models cached", set(c.model_cache.keys()) == {1, 2})
            await c.ensure_model(3)  # cache size is 2 -> model 1 (LRU) evicted
            ts.check("cache stays at capacity", len(c.model_cache) == 2)
            ts.check("least-recently-used model evicted", 1 not in c.model_cache and 3 in c.model_cache)
            await c.ensure_model(2)  # touch 2 -> now MRU
            await c.ensure_model(1)  # evicts 3 (LRU), keeps 2
            ts.check("touching a model protects it from eviction", set(c.model_cache.keys()) == {2, 1})
        finally:
            C.MODEL_CACHE_SIZE = saved_cap
    asyncio.run(go())


def test_process_model_scores_and_saves():
    async def go():
        pool = ts.FakePool(
            models={7: ts.FakeModel(0.5)},
            pending_ids=[101, 102],
            transcript_segments=["alpha beta gamma delta epsilon"],
        )
        c = _make_client(pool)
        C.execute_values = ts.make_capturing_execute_values(pool)
        c.send_data = _collect(c)
        c.client_state = "play"
        scored = await c.process_model(7)
        ts.check("scored every pending video", scored == 2)
        ts.check("two score rows saved", len(pool.saved) == 2)
        ts.check("rows tagged with model_id 7", all(r[1] == 7 for r in pool.saved))
        ts.check("score arrays non-empty", all(len(r[2]) > 0 for r in pool.saved))
        ts.check("model cached after use", 7 in c.model_cache)
    asyncio.run(go())


def test_request_model_assignment_and_no_work():
    async def go():
        pool = ts.FakePool(models={7: ts.FakeModel()})
        c = _make_client(pool)
        sent = []
        # Reply to each work_request like the server would.
        scripted = [{"packet_type": "assignment", "additional_data": {"model_id": 7}},
                    {"packet_type": "no_work", "additional_data": {}}]

        async def send(p):
            sent.append(p)
            if p["packet_type"] == "work_request":
                c.assignment_queue.put_nowait(scripted.pop(0))
        c.send_data = send

        await c.ensure_model(7)
        mid = await asyncio.wait_for(c.request_model(), 5)
        ts.check("assignment returns model id", mid == 7)
        wr = [p for p in sent if p["packet_type"] == "work_request"][-1]
        ts.check("work_request reports loaded models", wr["additional_data"]["loaded_models"] == [7])
        mid2 = await asyncio.wait_for(c.request_model(), 5)
        ts.check("no_work returns None", mid2 is None)
    asyncio.run(go())


def test_worker_loop_completes_model():
    async def go():
        pool = ts.FakePool(
            models={7: ts.FakeModel(0.5)},
            pending_ids=[101, 102],
            transcript_segments=["alpha beta gamma delta"],
        )
        c = _make_client(pool)
        C.execute_values = ts.make_capturing_execute_values(pool)
        sent = []
        scripted = [{"packet_type": "assignment", "additional_data": {"model_id": 7}},
                    {"packet_type": "no_work", "additional_data": {}}]

        async def send(p):
            sent.append(p)
            if p["packet_type"] == "work_request":
                reply = scripted.pop(0) if scripted else {"packet_type": "no_work", "additional_data": {}}
                if reply["packet_type"] == "no_work":
                    c.client_state = "pause"  # let the loop exit after draining
                c.assignment_queue.put_nowait(reply)
        c.send_data = send
        c.client_state = "play"

        await asyncio.wait_for(c.worker_loop(), 8)
        mc = [p for p in sent if p["packet_type"] == "model_complete"]
        ts.check("model_complete sent", bool(mc))
        ts.check("model_complete reports model + count",
                 mc[-1]["additional_data"] == {"model_id": 7, "scored_count": 2})
        ts.check("scores persisted", len(pool.saved) == 2)
    asyncio.run(go())


def _collect(client):
    async def send(_p):
        return None
    return send


def run_all_tests():
    print("\n=== ASYNC CLIENT (model-major) TESTS (offline) ===")
    test_ensure_model_lru_eviction()
    test_process_model_scores_and_saves()
    test_request_model_assignment_and_no_work()
    test_worker_loop_completes_model()
    print("All async client function tests passed.")


if __name__ == "__main__":
    run_all_tests()
