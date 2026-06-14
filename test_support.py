"""
test_support.py

Shared helpers for the OFFLINE unit tests. These tests do not touch a real
database or network: they exercise the new model-major scoring code against a
configurable in-memory fake of the psycopg2 connection-pool API.

Design notes:
  * stub_if_missing() only injects a stub for a dependency that isn't installed,
    so on a machine that actually has psycopg2 / nltk / psutil we use the real
    ones and never shadow them (important: other suites may make real DB calls).
  * FakeModel is defined at module top level so it can be pickled/unpickled,
    which is how the client loads models out of model_table.
  * FakePool mimics ThreadedConnectionPool.getconn()/putconn() and routes SQL by
    keyword to canned results, capturing anything written.
"""

import importlib
import pickle
import sys
import types


# ---------------------------------------------------------------------------
# Conditional stubs (only when a dependency is genuinely missing)
# ---------------------------------------------------------------------------

def _missing(modname):
    try:
        importlib.import_module(modname)
        return False
    except Exception:
        return True


def ensure_stubs():
    """Stub nltk / psutil / psycopg2 ONLY if they aren't importable."""
    if _missing("nltk") or _missing("nltk.tokenize") or _missing("nltk.util"):
        nk = types.ModuleType("nltk")
        nu = types.ModuleType("nltk.util")
        nt = types.ModuleType("nltk.tokenize")
        nt.word_tokenize = lambda s: s.split()

        def pad_sequence(seq, n, pad_left=False, pad_right=False,
                         left_pad_symbol=None, right_pad_symbol=None):
            seq = list(seq)
            if pad_left:
                seq = [left_pad_symbol] * (n - 1) + seq
            if pad_right:
                seq = seq + [right_pad_symbol] * (n - 1)
            return seq

        nu.pad_sequence = pad_sequence
        nk.util = nu
        nk.tokenize = nt
        sys.modules.update({"nltk": nk, "nltk.util": nu, "nltk.tokenize": nt})

    if _missing("psutil"):
        pu = types.ModuleType("psutil")
        pu.cpu_percent = lambda interval=None: 5.0
        pu.sensors_temperatures = lambda: {}

        class _VM:
            percent = 10.0

        pu.virtual_memory = lambda: _VM()
        sys.modules["psutil"] = pu

    if _missing("psycopg2"):
        ps = types.ModuleType("psycopg2")
        pl = types.ModuleType("psycopg2.pool")
        ex = types.ModuleType("psycopg2.extras")
        pl.ThreadedConnectionPool = lambda *a, **k: None
        ex.execute_values = lambda *a, **k: None
        ps.pool = pl
        ps.extras = ex
        ps.Error = Exception
        sys.modules.update({"psycopg2": ps, "psycopg2.pool": pl, "psycopg2.extras": ex})


# ---------------------------------------------------------------------------
# Picklable fake model (mimics an nltk language model's .score)
# ---------------------------------------------------------------------------

class FakeModel:
    def __init__(self, value=0.5):
        self.value = value

    def score(self, word, context):
        return self.value


# ---------------------------------------------------------------------------
# Fake connection pool / connection / cursor
# ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, store):
        self.store = store
        self._rows = []

    def execute(self, query, params=None):
        q = " ".join(query.lower().split())
        self._rows = []
        if "select id from model_table order by id" in q:
            self._rows = [(mid,) for mid in sorted(self.store["models"].keys())]
        elif "from vid_table" in q and "not exists" in q:
            # pending video ids for a model
            self._rows = [(vid,) for vid in self.store["pending_ids"]]
        elif "from vid_transcript_table" in q and "order by cum_word_count" in q:
            self._rows = [(seg,) for seg in self.store["transcript_segments"]]
        elif "from model_table where id" in q:
            ids = list(params[0]) if params else []
            self._rows = [
                (mid, f"key_{mid}", pickle.dumps(self.store["models"][mid]))
                for mid in ids if mid in self.store["models"]
            ]
        # writes (INSERT ... vid_score_table) go through execute_values, captured
        # separately by overriding the module's execute_values symbol.

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, store):
        self.store = store

    def cursor(self):
        return FakeCursor(self.store)

    def commit(self):
        self.store["commits"] += 1

    def rollback(self):
        self.store["rollbacks"] += 1

    def close(self):
        pass


class FakePool:
    """Mimics psycopg2 ThreadedConnectionPool getconn/putconn."""

    def __init__(self, models=None, pending_ids=None, transcript_segments=None):
        self.store = {
            "models": models or {},
            "pending_ids": pending_ids or [],
            "transcript_segments": transcript_segments or [],
            "saved": [],
            "commits": 0,
            "rollbacks": 0,
        }
        self.checked_out = 0

    def getconn(self):
        self.checked_out += 1
        return FakeConn(self.store)

    def putconn(self, conn):
        self.checked_out -= 1

    # convenience accessors
    @property
    def saved(self):
        return self.store["saved"]


def make_capturing_execute_values(pool):
    """A drop-in for psycopg2.extras.execute_values that records the rows."""
    def _execute_values(cur, sql, argslist, template=None, page_size=100):
        for row in argslist:
            pool.store["saved"].append(tuple(row))
    return _execute_values


# ---------------------------------------------------------------------------
# Tiny assertion/reporting helpers so suites print readable progress
# ---------------------------------------------------------------------------

def check(label, condition):
    if condition:
        print(f"  PASS  {label}")
    else:
        raise AssertionError(label)
