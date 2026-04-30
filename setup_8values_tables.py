"""
Create 8values-related tables on the vector Postgres DB (dev_pg_vector_db, etc.):

  - eightvalues_question          -- statement text + per-axis effect weights (from official quiz)
  - eightvalues_answer_option     -- Likert labels and multipliers (Strongly Agree ... Strongly Disagree)
  - eightvalues_rag_run           -- one row per RAG scoring run (model, scope, vid/channel, params)
  - eightvalues_rag_question_answer -- which answer option was chosen per question for that run
  - eightvalues_rag_axis_score    -- normalized 0-100 scores on the 4 axes for that run (+ raw sums / max)

Uses config/pg_vector_db_config.json for postgres connection.

Question text and effects are loaded from the upstream 8values questions.js unless --no-fetch
and config/eightvalues_questions.json exists.

Source reference: https://github.com/8values/8values.github.io
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

import psycopg2

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config" / "pg_vector_db_config.json"
LOCAL_QUESTIONS_PATH = ROOT / "config" / "eightvalues_questions.json"
UPSTREAM_QUESTIONS_JS = (
    "https://raw.githubusercontent.com/8values/8values.github.io/master/questions.js"
)

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS eightvalues_answer_option (
    answer_option_id SERIAL PRIMARY KEY,
    label VARCHAR(80) NOT NULL UNIQUE,
    multiplier NUMERIC(4,2) NOT NULL,
    sort_order SMALLINT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS eightvalues_question (
    question_id SERIAL PRIMARY KEY,
    quiz_index INTEGER NOT NULL UNIQUE,
    question_text TEXT NOT NULL,
    effect_econ SMALLINT NOT NULL DEFAULT 0,
    effect_dipl SMALLINT NOT NULL DEFAULT 0,
    effect_govt SMALLINT NOT NULL DEFAULT 0,
    effect_scty SMALLINT NOT NULL DEFAULT 0,
    source_version VARCHAR(128) NOT NULL DEFAULT '8values_github',
    insert_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS eightvalues_rag_run (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scope VARCHAR(32) NOT NULL,
    vid_id VARCHAR(255),
    channel_id VARCHAR(255),
    llm_model VARCHAR(255),
    embedding_model VARCHAR(255),
    rubric_version VARCHAR(128),
    top_k INTEGER,
    temperature NUMERIC(4,2),
    extra_params JSONB,
    notes TEXT,
    CONSTRAINT eightvalues_rag_run_scope_chk CHECK (
        scope IN ('video', 'channel', 'chunk_set', 'custom')
    )
);

CREATE TABLE IF NOT EXISTS eightvalues_rag_question_answer (
    answer_row_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES eightvalues_rag_run (run_id) ON DELETE CASCADE,
    question_id INTEGER NOT NULL REFERENCES eightvalues_question (question_id) ON DELETE RESTRICT,
    answer_option_id INTEGER NOT NULL REFERENCES eightvalues_answer_option (answer_option_id) ON DELETE RESTRICT,
    raw_llm_response TEXT,
    evidence_chunk_ids INTEGER[],
    confidence NUMERIC(5,4),
    insert_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, question_id)
);

CREATE TABLE IF NOT EXISTS eightvalues_rag_axis_score (
    score_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL UNIQUE REFERENCES eightvalues_rag_run (run_id) ON DELETE CASCADE,
    score_econ_pct NUMERIC(7,3),
    score_dipl_pct NUMERIC(7,3),
    score_govt_pct NUMERIC(7,3),
    score_scty_pct NUMERIC(7,3),
    raw_sum_econ INTEGER,
    raw_sum_dipl INTEGER,
    raw_sum_govt INTEGER,
    raw_sum_scty INTEGER,
    max_abs_econ INTEGER NOT NULL,
    max_abs_dipl INTEGER NOT NULL,
    max_abs_govt INTEGER NOT NULL,
    max_abs_scty INTEGER NOT NULL,
    insert_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_eightvalues_run_created ON eightvalues_rag_run (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_eightvalues_run_vid ON eightvalues_rag_run (vid_id);",
    "CREATE INDEX IF NOT EXISTS idx_eightvalues_run_channel ON eightvalues_rag_run (channel_id);",
    "CREATE INDEX IF NOT EXISTS idx_eightvalues_qa_run ON eightvalues_rag_question_answer (run_id);",
]


def load_postgres_params() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    pg = cfg.get("postgres")
    if not isinstance(pg, dict):
        raise SystemExit("pg_vector_db_config.json must contain a 'postgres' object")
    return pg


def fetch_questions_from_github() -> List[Dict[str, Any]]:
    req = urllib.request.Request(UPSTREAM_QUESTIONS_JS, headers={"User-Agent": "transcript_analysis_setup"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        text = resp.read().decode("utf-8")
    m = re.search(r"questions\s*=\s*(\[[\s\S]*?\])\s*;", text)
    if not m:
        raise ValueError("Could not parse questions array from upstream questions.js")
    return json.loads(m.group(1))


def load_questions_local_or_fetch(no_fetch: bool) -> List[Dict[str, Any]]:
    if no_fetch and LOCAL_QUESTIONS_PATH.is_file():
        with open(LOCAL_QUESTIONS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    if no_fetch:
        raise SystemExit(f"--no-fetch but {LOCAL_QUESTIONS_PATH} not found")
    logger.info("Fetching questions from %s", UPSTREAM_QUESTIONS_JS)
    return fetch_questions_from_github()


def seed_answer_options(cur) -> None:
    rows = [
        ("Strongly Agree", 1.0, 1),
        ("Agree", 0.5, 2),
        ("Neutral/Unsure", 0.0, 3),
        ("Disagree", -0.5, 4),
        ("Strongly Disagree", -1.0, 5),
    ]
    for label, mult, so in rows:
        cur.execute(
            """
            INSERT INTO eightvalues_answer_option (label, multiplier, sort_order)
            VALUES (%s, %s, %s)
            ON CONFLICT (sort_order) DO UPDATE SET
                label = EXCLUDED.label,
                multiplier = EXCLUDED.multiplier
            """,
            (label, mult, so),
        )


def seed_questions(cur, questions: List[Dict[str, Any]], source_version: str) -> None:
    for idx, item in enumerate(questions):
        eff = item.get("effect") or {}
        cur.execute(
            """
            INSERT INTO eightvalues_question (
                quiz_index, question_text,
                effect_econ, effect_dipl, effect_govt, effect_scty,
                source_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (quiz_index) DO UPDATE SET
                question_text = EXCLUDED.question_text,
                effect_econ = EXCLUDED.effect_econ,
                effect_dipl = EXCLUDED.effect_dipl,
                effect_govt = EXCLUDED.effect_govt,
                effect_scty = EXCLUDED.effect_scty,
                source_version = EXCLUDED.source_version
            """,
            (
                idx,
                item["question"],
                int(eff.get("econ", 0)),
                int(eff.get("dipl", 0)),
                int(eff.get("govt", 0)),
                int(eff.get("scty", 0)),
                source_version,
            ),
        )


def run_setup(seed: bool, no_fetch: bool, save_local_questions: bool) -> None:
    params = load_postgres_params()
    questions = load_questions_local_or_fetch(no_fetch) if seed else []

    if save_local_questions and questions:
        LOCAL_QUESTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LOCAL_QUESTIONS_PATH, "w", encoding="utf-8") as f:
            json.dump(questions, f, indent=2, ensure_ascii=False)
        logger.info("Wrote %s", LOCAL_QUESTIONS_PATH)

    conn = psycopg2.connect(**params)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(DDL)
            for stmt in INDEX_DDL:
                cur.execute(stmt)

        if seed:
            source_ver = "8values_github_master"
            with conn.cursor() as cur:
                seed_answer_options(cur)
                seed_questions(cur, questions, source_ver)
            conn.commit()
            logger.info(
                "Seeded %s answer options and %s questions.",
                5,
                len(questions),
            )
        else:
            logger.info("DDL applied; skipped seed (--no-seed).")

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Create 8values RAG scoring tables on vector Postgres")
    parser.add_argument(
        "--no-seed",
        action="store_true",
        help="Only create tables/indexes; do not load questions or answer options",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Load questions from config/eightvalues_questions.json instead of GitHub",
    )
    parser.add_argument(
        "--save-local-questions",
        action="store_true",
        help="When fetching from GitHub, also write config/eightvalues_questions.json",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        run_setup(seed=not args.no_seed, no_fetch=args.no_fetch, save_local_questions=args.save_local_questions)
    except Exception as e:
        logger.error("%s", e)
        sys.exit(1)

    print("[OK] eightvalues tables setup finished.")


if __name__ == "__main__":
    main()
