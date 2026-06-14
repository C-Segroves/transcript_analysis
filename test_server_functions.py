"""
Offline unit tests for the model-major coordination in
server/async_processing_server.py (ProcessingServer).

No database or network required. Verifies the in-memory assignment logic:
  * pick_model_for: no model assigned to two clients at once
  * resume: a client that already owns an unfinished model gets it back
  * affinity: prefer a model the client already has loaded
  * handle_model_complete marks completion and frees the model
  * release_client frees a disconnected client's models
  * get_worker_snapshot reports sane counts

Run directly:  python test_server_functions.py
Or via:        python run_all_tests.py
"""

import os
import sys
import logging

root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root_dir)
sys.path.insert(0, os.path.join(root_dir, "server"))

import test_support as ts
ts.ensure_stubs()

# async_processing_server imports maintain_database (heavy) and dashboard; stub
# maintain_database so the import is light. dashboard imports cleanly (stdlib +
# psycopg2, already handled by ensure_stubs).
import types as _types


def _stub_module_if_unimportable(name, build):
    if name in sys.modules:
        return
    try:
        __import__(name)
    except Exception:
        sys.modules[name] = build()


def _build_maintain():
    m = _types.ModuleType("maintain_database")
    m.maintain_database = lambda *a, **k: None
    return m


def _build_dashboard():
    m = _types.ModuleType("dashboard")
    m.start_dashboard_in_thread = lambda *a, **k: None
    return m


# async_processing_server imports these at module load. The coordination tests
# don't use them; stub only if they aren't importable in this environment.
_stub_module_if_unimportable("maintain_database", _build_maintain)
_stub_module_if_unimportable("dashboard", _build_dashboard)

import async_processing_server as S
# The class reads a module-global db_pool in __init__; it's only created in
# __main__, so provide a placeholder for the tests.
S.db_pool = None

_LOG = logging.getLogger("test_server")
_LOG.setLevel(logging.CRITICAL)  # keep the server's own logging quiet (don't disable globally)


def _server(model_ids):
    srv = S.ProcessingServer("h", 5000, _LOG, {})
    srv.all_model_ids = list(model_ids)
    return srv


def test_no_double_assignment():
    srv = _server([1, 2, 3])
    a, b = object(), object()
    m1 = srv.pick_model_for(a, [])
    m2 = srv.pick_model_for(b, [])
    ts.check("two clients get different models", m1 != m2)
    ts.check("each model has one owner", srv.model_owner[m1] is a and srv.model_owner[m2] is b)


def test_resume_owned_model():
    srv = _server([1, 2, 3])
    a = object()
    m = srv.pick_model_for(a, [])
    again = srv.pick_model_for(a, [99])  # still owns m, unfinished -> resume
    ts.check("client resumes its unfinished model", again == m)


def test_affinity_to_loaded():
    srv = _server([1, 2, 3])
    a, b = object(), object()
    srv.pick_model_for(a, [])          # a takes model 1
    srv.handle_model_complete(b, None)  # no-op guard
    # b has model 3 loaded; 1 is owned by a, so b should get 3 (affinity), not 2
    choice = srv.pick_model_for(b, [3])
    ts.check("affinity picks an already-loaded model", choice == 3)


def test_complete_and_release():
    srv = _server([1, 2, 3])
    a, b, c = object(), object(), object()
    m_a = srv.pick_model_for(a, [])   # 1
    m_b = srv.pick_model_for(b, [])   # 2
    srv.handle_model_complete(b, m_b, scored_count=5)
    ts.check("completed model recorded", m_b in srv.completed_models)
    ts.check("completed model freed from owner", m_b not in srv.model_owner)
    # c can't get anything: 1 owned by a, 3 free... actually 3 is free
    got = srv.pick_model_for(c, [])
    ts.check("remaining free model assigned to new client", got == 3)
    # now everything is owned (1=a,3=c) or complete (2); next client gets nothing
    d = object()
    ts.check("nothing left to assign", srv.pick_model_for(d, []) is None)
    # a disconnects -> model 1 freed
    srv.release_client(a)
    ts.check("released model becomes assignable", srv.pick_model_for(d, []) == m_a)


def test_snapshot():
    srv = _server([1, 2, 3])
    a = object()
    srv._client_record(a)["machine_name"] = "boxA"
    srv.pick_model_for(a, [])
    snap = srv.get_worker_snapshot()
    ts.check("snapshot totals models", snap["total_models"] == 3)
    ts.check("snapshot counts busy models", snap["busy_models"] == 1)
    ts.check("snapshot lists the client", any(c["machine_name"] == "boxA" for c in snap["clients"]))


def run_all_tests():
    print("\n=== SERVER COORDINATION TESTS (offline) ===")
    test_no_double_assignment()
    test_resume_owned_model()
    test_affinity_to_loaded()
    test_complete_and_release()
    test_snapshot()
    print("All server function tests passed.")


if __name__ == "__main__":
    run_all_tests()
