"""
run_8values_survey.py

Run the 8values survey: ask an LLM (via Ollama) each of the 70 questions about a
subject, record the answers, and compute the 4-axis 8values score.

Modes (column eightvalues_run.mode):
  transcript     question + top-k retrieved transcript chunks (per video x model x repeat)
  baseline       question only, no context (per model x repeat) -- the model's own lean
  probe_for      question + the 'for' statement   (per model x repeat) -- can it swing pro?
  probe_against  question + the 'against' statement(per model x repeat) -- can it swing con?

Two subcommands:
  seed   create eightvalues_run rows (the work queue)
  work   claim pending runs (FOR UPDATE SKIP LOCKED) and process them

Refusals/parse failures are retried up to --max-retries; if still unresolved the
question is recorded as failed (raw output kept, plus every attempt) and scored
as Neutral. config/eightvalues_prompt.md supplies the system/user prompt.

Config / env:
  OLLAMA_HOST  default http://localhost:11434
  EMBED_MODEL  default nomic-embed-text   (for transcript retrieval)
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import urllib.request

import psycopg2
from psycopg2.extras import Json, execute_values

ROOT = os.path.dirname(os.path.abspath(__file__))
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")
PROMPT_PATH = os.path.join(ROOT, "config", "eightvalues_prompt.md")

AXES = ("econ", "dipl", "govt", "scty")


# ---------------------------------------------------------------------------
# Config / logging / prompt
# ---------------------------------------------------------------------------

def load_db_config():
    with open(os.path.join(ROOT, "config", "db_config.json"), "r") as f:
        return json.load(f)


def setup_logger():
    logger = logging.getLogger("Survey8v")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(h)
    return logger


_JSON_TAIL = (
    'Reply with ONLY a single-line JSON object and nothing else:\n'
    '{"answer": "<one of the five labels, exactly>", "confidence": <0.0-1.0>, "reason": "<15 words max>"}'
)
_FIVE = '  "Strongly Agree", "Agree", "Neutral/Unsure", "Disagree", "Strongly Disagree"'

# --- transcript mode: infer how the speaker in the excerpts would answer -------
DEFAULT_SYSTEM = (
    "You are a meticulous political-science research assistant doing content analysis. "
    "You infer how a speaker would respond to a statement based only on provided excerpts. "
    "Always choose exactly one of the five allowed answers and reply with only the requested JSON."
)
DEFAULT_USER = (
    'TRANSCRIPT EXCERPTS (one speaker):\n"""\n{{CONTEXT}}\n"""\n\n'
    'STATEMENT TO ASSESS:\n"{{STATEMENT}}"\n\n'
    'Reply with ONLY a single-line JSON object:\n'
    '{"answer": "<Strongly Agree|Agree|Neutral/Unsure|Disagree|Strongly Disagree>", '
    '"confidence": <0.0-1.0>, "reason": "<15 words max>"}'
)

# --- baseline mode: the model's OWN unprompted answer, no context --------------
BASELINE_SYSTEM = (
    "You are answering a political-values questionnaire as yourself. Give your own "
    "honest assessment of each statement. You must choose exactly one of the five "
    "allowed answers; use 'Neutral/Unsure' only when you genuinely have no lean, not "
    "to avoid answering. Output only the requested JSON, with no preamble."
)
BASELINE_USER = (
    'STATEMENT:\n"{{STATEMENT}}"\n\n'
    'Do you agree or disagree? Choose exactly one:\n' + _FIVE + '\n\n' + _JSON_TAIL
)

# --- probe mode: role-play a strong for/against stance, then answer ------------
PROBE_SYSTEM = (
    "You are role-playing a person who strongly and sincerely holds a specified "
    "viewpoint. Answer each statement exactly as THAT person would, staying fully in "
    "character. You must choose exactly one of the five allowed answers. Output only "
    "the requested JSON, with no preamble."
)
PROBE_USER = (
    'You strongly and sincerely hold the following position:\n"""\n{{CONTEXT}}\n"""\n\n'
    'Staying in character as someone who holds that position, how do you respond to '
    'this statement?\nSTATEMENT:\n"{{STATEMENT}}"\n\n'
    'Choose exactly one:\n' + _FIVE + '\n\n' + _JSON_TAIL
)

_DEFAULTS = {
    "transcript": (DEFAULT_SYSTEM, DEFAULT_USER),
    "baseline": (BASELINE_SYSTEM, BASELINE_USER),
    "probe": (PROBE_SYSTEM, PROBE_USER),
}


def load_prompt_templates():
    """Return {mode_key: (system, user)} for transcript / baseline / probe.

    Defaults live in code; the fenced blocks in eightvalues_prompt.md override them
    if present: ## SYSTEM / ## USER (transcript), ## BASELINE_SYSTEM / ## BASELINE_USER,
    ## PROBE_SYSTEM / ## PROBE_USER. probe_for and probe_against both use 'probe'.
    """
    try:
        with open(PROMPT_PATH, "r", encoding="utf-8") as f:
            md = f.read()
    except Exception:
        return dict(_DEFAULTS)

    def block_after(header):
        m = re.search(re.escape(header) + r"\s*```[a-zA-Z]*\n(.*?)```", md, re.DOTALL)
        return m.group(1).strip() if m else None

    return {
        "transcript": (block_after("## SYSTEM") or DEFAULT_SYSTEM,
                       block_after("## USER") or DEFAULT_USER),
        "baseline": (block_after("## BASELINE_SYSTEM") or BASELINE_SYSTEM,
                     block_after("## BASELINE_USER") or BASELINE_USER),
        "probe": (block_after("## PROBE_SYSTEM") or PROBE_SYSTEM,
                  block_after("## PROBE_USER") or PROBE_USER),
    }


def template_for_mode(templates, mode):
    """probe_for / probe_against share the 'probe' template."""
    key = "probe" if mode in ("probe_for", "probe_against") else mode
    return templates.get(key, templates["transcript"])


# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------

def _post(path, payload, timeout):
    req = urllib.request.Request(
        f"{OLLAMA_HOST}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def ollama_chat(model, system, user, temperature, logger):
    try:
        data = _post("/api/chat", {
            "model": model,
            "messages": [{"role": "system", "content": system},
                         {"role": "user", "content": user}],
            "stream": False,
            "options": {"temperature": temperature},
        }, timeout=300)
        return (data.get("message") or {}).get("content", "")
    except Exception as e:
        logger.error("Ollama chat failed: %s", e)
        return ""


def ollama_embed(text, logger):
    try:
        data = _post("/api/embeddings", {"model": EMBED_MODEL, "prompt": text}, timeout=120)
        return data.get("embedding")
    except Exception as e:
        logger.error("Ollama embed failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

def load_questions(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, quiz_index, question_text, effect_econ, effect_dipl, effect_govt, effect_scty "
            "FROM eightvalues_question ORDER BY quiz_index"
        )
        return [
            {"id": r[0], "quiz_index": r[1], "text": r[2],
             "econ": r[3], "dipl": r[4], "govt": r[5], "scty": r[6]}
            for r in cur.fetchall()
        ]


def load_answer_options(conn):
    """Return (by_label dict, by_order dict) of {normalized -> (option_id, multiplier)}."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, sort_order, label, multiplier FROM eightvalues_answer_option")
        rows = cur.fetchall()
    by_label, by_order = {}, {}
    for oid, order, label, mult in rows:
        by_label[_norm(label)] = (oid, float(mult))
        by_order[int(order)] = (oid, float(mult))
    return by_label, by_order


def _norm(s):
    return re.sub(r"\s+", "", str(s).lower()).replace("/", "")


def parse_answer(raw, by_label, by_order):
    """Return (option_id, multiplier, confidence, reason) or (None, None, conf, reason) if unmatched."""
    confidence, reason, answer = None, None, None
    # Try to pull a JSON object out of the text.
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        try:
            obj = json.loads(m.group(0))
            answer = obj.get("answer")
            confidence = obj.get("confidence")
            reason = obj.get("reason")
        except Exception:
            pass

    # Match longest label first: "Strongly Agree" before "Agree", "Disagree"
    # before "Agree" (substring traps), etc.
    labels_by_len = sorted(by_label.items(), key=lambda kv: -len(kv[0]))

    def match_label(text):
        key = _norm(text)
        for lbl, val in labels_by_len:
            if lbl and lbl in key:
                return val
        return None

    # 1) Prefer the explicit JSON "answer" field (label, or a bare 1-5).
    if answer is not None:
        v = match_label(str(answer))
        if v:
            return v[0], v[1], confidence, reason
        m2 = re.search(r"\b([1-5])\b", str(answer))
        if m2:
            oid, mult = by_order[int(m2.group(1))]
            return oid, mult, confidence, reason

    # 2) Fall back to a label appearing anywhere in the raw text (labels only,
    #    to avoid stray digits like a confidence value being read as an answer).
    v = match_label(raw)
    if v:
        return v[0], v[1], confidence, reason
    return None, None, confidence, reason


# ---------------------------------------------------------------------------
# Context retrieval
# ---------------------------------------------------------------------------

def cosine_top_k(question_vec, chunks, k):
    """chunks: list of (text, embedding_list). Return top-k texts by cosine similarity."""
    import numpy as np
    if not chunks:
        return []
    q = np.asarray(question_vec, dtype=float)
    qn = q / (np.linalg.norm(q) + 1e-9)
    scored = []
    for text, emb in chunks:
        v = np.asarray(emb, dtype=float)
        sim = float(np.dot(qn, v / (np.linalg.norm(v) + 1e-9)))
        scored.append((sim, text))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [t for _, t in scored[:k]]


def get_vid_chunks(conn, vid_id):
    with conn.cursor() as cur:
        cur.execute("SELECT text, embedding FROM transcript_chunk_table WHERE vid_id = %s", (vid_id,))
        return cur.fetchall()


def get_probe_statement(conn, question_id, direction, variant):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT statement_text FROM eightvalues_probe_statement "
            "WHERE question_id = %s AND direction = %s AND variant = %s",
            (question_id, direction, variant),
        )
        row = cur.fetchone()
        return row[0] if row else ""


# ---------------------------------------------------------------------------
# Scoring (standard 8values)
# ---------------------------------------------------------------------------

def compute_axis_scores(questions, multipliers):
    """
    questions: list of {econ,dipl,govt,scty}. multipliers: dict question_id -> multiplier
    (missing / None => Neutral => 0). Returns (pct, raw, maxv) dicts keyed by axis.
    """
    raw = {a: 0.0 for a in AXES}
    maxv = {a: 0.0 for a in AXES}
    for q in questions:
        mult = multipliers.get(q["id"])
        if mult is None:
            mult = 0.0
        for a in AXES:
            eff = q[a]
            raw[a] += eff * mult
            maxv[a] += abs(eff)
    pct = {}
    for a in AXES:
        pct[a] = round(100.0 * (maxv[a] + raw[a]) / (2 * maxv[a]), 3) if maxv[a] else 50.0
    return pct, raw, maxv


# ---------------------------------------------------------------------------
# Processing one run
# ---------------------------------------------------------------------------

def build_context(conn, run, question, qvec_cache, logger):
    mode = run["mode"]
    if mode == "baseline":
        return ""
    variant = run.get("probe_variant") or "verbose"
    if mode == "probe_for":
        return get_probe_statement(conn, question["id"], "for", variant)
    if mode == "probe_against":
        return get_probe_statement(conn, question["id"], "against", variant)
    # transcript: retrieve top-k chunks for this video by similarity to the question
    chunks = get_vid_chunks(conn, run["vid_id"])
    if not chunks:
        raise RuntimeError(f"vid_id={run['vid_id']} has no transcript chunks (embed it first).")
    qvec = qvec_cache.get(question["id"])
    if qvec is None:
        qvec = ollama_embed(question["text"], logger)
        qvec_cache[question["id"]] = qvec
    if qvec is None:
        raise RuntimeError("could not embed question for retrieval (Ollama embed down?).")
    top = cosine_top_k(qvec, chunks, run["top_k"] or 6)
    return "\n\n".join(top)


def answer_one_question(model, system_tmpl, user_tmpl, context, statement, temperature,
                        by_label, by_order, max_retries, logger):
    """Returns dict: option_id, multiplier, failed, attempts, confidence, reason, raw, all_outputs."""
    attempts = 0
    all_outputs = []
    user = user_tmpl.replace("{{CONTEXT}}", context or "(no excerpts provided)").replace("{{STATEMENT}}", statement)
    for attempt in range(1, max_retries + 1):
        attempts = attempt
        raw = ollama_chat(model, system_tmpl, user, temperature, logger)
        all_outputs.append(raw)
        oid, mult, conf, reason = parse_answer(raw, by_label, by_order)
        if oid is not None:
            return {"option_id": oid, "multiplier": mult, "failed": False, "attempts": attempts,
                    "confidence": conf, "reason": reason, "raw": raw, "all_outputs": all_outputs}
    # Exhausted retries -> failed, scored Neutral.
    return {"option_id": None, "multiplier": None, "failed": True, "attempts": attempts,
            "confidence": None, "reason": None, "raw": all_outputs[-1] if all_outputs else "",
            "all_outputs": all_outputs}


def store_response(conn, run_id, question_id, res):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO eightvalues_response
                (run_id, question_id, answer_option_id, failed, attempts, confidence, reason, raw_output, all_outputs)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, question_id) DO UPDATE SET
                answer_option_id = EXCLUDED.answer_option_id, failed = EXCLUDED.failed,
                attempts = EXCLUDED.attempts, confidence = EXCLUDED.confidence, reason = EXCLUDED.reason,
                raw_output = EXCLUDED.raw_output, all_outputs = EXCLUDED.all_outputs
            """,
            (run_id, question_id, res["option_id"], res["failed"], res["attempts"],
             res["confidence"], res["reason"], res["raw"], Json(res["all_outputs"])),
        )


def store_score(conn, run_id, pct, raw, maxv, answered, failed):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO eightvalues_score
                (run_id, score_econ, score_dipl, score_govt, score_scty,
                 raw_econ, raw_dipl, raw_govt, raw_scty, max_econ, max_dipl, max_govt, max_scty,
                 answered_count, failed_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                score_econ = EXCLUDED.score_econ, score_dipl = EXCLUDED.score_dipl,
                score_govt = EXCLUDED.score_govt, score_scty = EXCLUDED.score_scty,
                raw_econ = EXCLUDED.raw_econ, raw_dipl = EXCLUDED.raw_dipl,
                raw_govt = EXCLUDED.raw_govt, raw_scty = EXCLUDED.raw_scty,
                max_econ = EXCLUDED.max_econ, max_dipl = EXCLUDED.max_dipl,
                max_govt = EXCLUDED.max_govt, max_scty = EXCLUDED.max_scty,
                answered_count = EXCLUDED.answered_count, failed_count = EXCLUDED.failed_count
            """,
            (run_id, pct["econ"], pct["dipl"], pct["govt"], pct["scty"],
             raw["econ"], raw["dipl"], raw["govt"], raw["scty"],
             maxv["econ"], maxv["dipl"], maxv["govt"], maxv["scty"], answered, failed),
        )


def process_run(conn, run, questions, by_label, by_order, templates,
                max_retries, qvec_cache, logger):
    system_tmpl, user_tmpl = template_for_mode(templates, run["mode"])
    multipliers = {}
    failed = 0
    for q in questions:
        context = build_context(conn, run, q, qvec_cache, logger)
        res = answer_one_question(run["llm_model"], system_tmpl, user_tmpl, context, q["text"],
                                  float(run["temperature"]) if run["temperature"] is not None else 0.0,
                                  by_label, by_order, max_retries, logger)
        multipliers[q["id"]] = res["multiplier"]  # None if failed
        if res["failed"]:
            failed += 1
        store_response(conn, run["id"], q["id"], res)
    conn.commit()

    pct, raw, maxv = compute_axis_scores(questions, multipliers)
    answered = len(questions) - failed
    store_score(conn, run["id"], pct, raw, maxv, answered, failed)
    conn.commit()
    return failed, pct


# ---------------------------------------------------------------------------
# Queue: claim / seed
# ---------------------------------------------------------------------------

CLAIM_SQL = """
WITH claimed AS (
    SELECT id FROM eightvalues_run
    WHERE status = 'pending' OR (status = 'error' AND attempts < %(max_attempts)s)
    ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1
)
UPDATE eightvalues_run r
SET status = 'in_progress', attempts = r.attempts + 1, claimed_at = now(), updated_at = now()
FROM claimed WHERE r.id = claimed.id
RETURNING r.id, r.mode, r.vid_id, r.llm_model, r.repeat_index, r.top_k, r.temperature, r.probe_variant;
"""


def claim_run(conn, max_attempts):
    with conn.cursor() as cur:
        cur.execute(CLAIM_SQL, {"max_attempts": max_attempts})
        row = cur.fetchone()
    conn.commit()
    if not row:
        return None
    return {"id": row[0], "mode": row[1], "vid_id": row[2], "llm_model": row[3],
            "repeat_index": row[4], "top_k": row[5], "temperature": row[6],
            "probe_variant": row[7]}


def seed_runs(conn, mode, channel, models, repeats, top_k, temperature, probe_variant, logger):
    """Create pending eightvalues_run rows. probe_variant tags probe runs only."""
    modes = ["baseline", "probe_for", "probe_against"] if mode == "calibration" else [mode]
    total = 0
    for m in modes:
        # Only probe runs carry a variant; baseline/transcript leave it NULL.
        pv = probe_variant if m in ("probe_for", "probe_against") else None
        if m == "transcript":
            # one run per (video with chunks) x model x repeat
            where = ["EXISTS (SELECT 1 FROM transcript_chunk_table tc WHERE tc.vid_id = v.id)"]
            params = []
            if channel:
                where.append("v.channel_id IN (SELECT id FROM channel_table WHERE yt_channel_id = %s OR channel_handle = %s)")
                params.extend([channel, channel.lstrip("@")])
            with conn.cursor() as cur:
                cur.execute(f"SELECT v.id FROM vid_table v WHERE {' AND '.join(where)} ORDER BY v.id", params)
                vids = [r[0] for r in cur.fetchall()]
            rows = [(m, vid, model, rep, top_k, temperature, pv)
                    for vid in vids for model in models for rep in range(1, repeats + 1)]
        else:
            # baseline / probe: per model x repeat, no video
            rows = [(m, None, model, rep, top_k, temperature, pv)
                    for model in models for rep in range(1, repeats + 1)]
        if rows:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO eightvalues_run "
                    "(mode, vid_id, llm_model, repeat_index, top_k, temperature, probe_variant) "
                    "VALUES %s ON CONFLICT (mode, COALESCE(vid_id, -1), llm_model, repeat_index, "
                    "COALESCE(probe_variant, '')) DO NOTHING",
                    rows,
                )
            conn.commit()
        logger.info("Seeded %d %s run(s)%s.", len(rows), m, f" [{pv}]" if pv else "")
        total += len(rows)
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def cmd_seed(args, logger):
    conn = psycopg2.connect(**load_db_config())
    try:
        models = [m.strip() for m in args.models.split(",") if m.strip()]
        seed_runs(conn, args.mode, args.channel, models, args.repeats, args.top_k,
                  args.temperature, args.probe_variant, logger)
    finally:
        conn.close()


def cmd_work(args, logger):
    conn = psycopg2.connect(**load_db_config())
    try:
        questions = load_questions(conn)
        by_label, by_order = load_answer_options(conn)
        templates = load_prompt_templates()
        qvec_cache = {}
        processed = 0
        while True:
            run = claim_run(conn, args.max_attempts)
            if run is None:
                if args.once:
                    break
                time.sleep(args.poll_seconds)
                continue
            logger.info("Run %s: mode=%s vid=%s model=%s repeat=%s",
                        run["id"], run["mode"], run["vid_id"], run["llm_model"], run["repeat_index"])
            try:
                failed, pct = process_run(conn, run, questions, by_label, by_order,
                                          templates, args.max_retries, qvec_cache, logger)
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE eightvalues_run SET status='done', failed_count=%s, error=NULL, updated_at=now() WHERE id=%s",
                        (failed, run["id"]),
                    )
                conn.commit()
                logger.info("Run %s done. failed=%d scores=%s", run["id"], failed, pct)
                processed += 1
            except Exception as e:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("UPDATE eightvalues_run SET status='error', error=%s, updated_at=now() WHERE id=%s",
                                (str(e)[:1000], run["id"]))
                conn.commit()
                logger.warning("Run %s failed: %s", run["id"], e)
        logger.info("Worker done. Processed %d run(s).", processed)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="8values LLM survey: seed runs / process runs.")
    sub = parser.add_subparsers(dest="command", required=True)

    s = sub.add_parser("seed", help="Create eightvalues_run rows (work queue).")
    s.add_argument("--mode", required=True,
                   choices=["transcript", "baseline", "probe_for", "probe_against", "calibration"],
                   help="'calibration' = baseline + both probes")
    s.add_argument("--channel", help="For transcript mode: limit to this channel (yt_channel_id or @handle)")
    s.add_argument("--models", required=True, help="Comma-separated Ollama model names")
    s.add_argument("--repeats", type=int, default=1, help="Repeats per subject")
    s.add_argument("--top-k", type=int, default=6, help="Chunks retrieved per question (transcript mode)")
    s.add_argument("--temperature", type=float, default=0.0)
    s.add_argument("--probe-variant", choices=["verbose", "succinct"], default="verbose",
                   help="Which probe statement set to use for probe_for/probe_against")

    w = sub.add_parser("work", help="Claim and process pending runs.")
    w.add_argument("--once", action="store_true", help="Drain the queue then exit")
    w.add_argument("--poll-seconds", type=int, default=15)
    w.add_argument("--max-retries", type=int, default=5, help="Retries per question on refusal/parse-fail")
    w.add_argument("--max-attempts", type=int, default=3, help="Max claim attempts before leaving an errored run")

    args = parser.parse_args()
    logger = setup_logger()
    if args.command == "seed":
        cmd_seed(args, logger)
    else:
        cmd_work(args, logger)


if __name__ == "__main__":
    main()
