"""
build_succinct_probes.py

Deterministically build the *succinct* 8values probe statements -- no LLM.
For each question, lowercase the question text and prepend a fixed stance phrase:

  "for"     -> "I strongly believe that "         + <question lowercased>
  "against" -> "I strongly oppose the idea that "  + <question lowercased>

Writes config/eightvalues_probe_statements_succinct.json: a list aligned to
quiz_index, each {"for": ..., "against": ...} -- the same file the succinct
probe variant is seeded from.

Usage:
    python build_succinct_probes.py
"""

import json
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
QUESTIONS_PATH = os.path.join(ROOT, "config", "eightvalues_questions.json")
OUT_PATH = os.path.join(ROOT, "config", "eightvalues_probe_statements_succinct.json")

FOR_PREFIX = "I strongly believe that "
AGAINST_PREFIX = "I strongly oppose the idea that "


def main():
    with open(QUESTIONS_PATH, "r", encoding="utf-8") as f:
        questions = json.load(f)

    out = []
    for item in questions:
        q = item["question"].lower()
        out.append({
            "for": FOR_PREFIX + q,
            "against": AGAINST_PREFIX + q,
        })

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"Wrote {OUT_PATH}: {len(out)} entries (no LLM).")
    if out:
        print("Example (quiz_index 0):")
        print("  for:     " + out[0]["for"])
        print("  against: " + out[0]["against"])


if __name__ == "__main__":
    main()
