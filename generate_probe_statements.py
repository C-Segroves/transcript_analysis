"""
generate_probe_statements.py

Draft the per-question 'for' / 'against' probe statements with an LLM and write
them to config/eightvalues_probe_statements.json (aligned to quiz_index).

These are used by the probe_for / probe_against calibration runs: each is a short
passage that a Strongly-Agree (for) or Strongly-Disagree (against) speaker might
say about the question's topic, so you can verify a model will swing to both
extremes.

REVIEW the output before seeding -- generated statements should be checked for
quality and balance. Then: python setup_8values.py  (re-seeds from the JSON).

Usage:
  OLLAMA_HOST=http://gpu-box:11434 python generate_probe_statements.py --model qwen2.5:7b-instruct
  python generate_probe_statements.py --model llama3.1:8b --overwrite
"""

import argparse
import json
import os
import re
import sys
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
QUESTIONS_PATH = os.path.join(ROOT, "config", "eightvalues_questions.json")
OUT_PATH = os.path.join(ROOT, "config", "eightvalues_probe_statements.json")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")

SYSTEM = (
    "You are helping build a neutral political-science test set. For a given survey "
    "statement you will write two short first-person passages (2-4 sentences each), "
    "as if spoken in a talk: one from someone who STRONGLY AGREES with the statement, "
    "and one from someone who STRONGLY DISAGREES. Make each clearly and strongly committed "
    "to its side, plausible and non-cartoonish. This is for testing a classifier; present "
    "both sides fairly. Reply with ONLY JSON: {\"for\": \"...\", \"against\": \"...\"}."
)


def chat(model, statement, timeout=300):
    user = f'SURVEY STATEMENT:\n"{statement}"\n\nWrite the two passages now as JSON.'
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}],
        "stream": False,
        "options": {"temperature": 0.7},
    }).encode("utf-8")
    req = urllib.request.Request(f"{OLLAMA_HOST}/api/chat", data=payload,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.load(resp)
    content = (data.get("message") or {}).get("content", "")
    m = re.search(r"\{.*\}", content, re.DOTALL)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
        return {"for": obj.get("for", ""), "against": obj.get("against", "")}
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description="Generate 8values probe statements with an LLM.")
    parser.add_argument("--model", required=True, help="Ollama model to draft with")
    parser.add_argument("--overwrite", action="store_true", help="Regenerate entries that already exist")
    args = parser.parse_args()

    with open(QUESTIONS_PATH, "r", encoding="utf-8") as f:
        questions = json.load(f)

    existing = []
    if os.path.exists(OUT_PATH):
        with open(OUT_PATH, "r", encoding="utf-8") as f:
            existing = json.load(f)
    # pad existing to length
    out = list(existing) + [{} for _ in range(len(questions) - len(existing))]

    for idx, item in enumerate(questions):
        if out[idx].get("for") and out[idx].get("against") and not args.overwrite:
            continue
        statement = item["question"]
        print(f"[{idx+1}/{len(questions)}] {statement[:70]}...", file=sys.stderr)
        result = chat(args.model, statement)
        if result:
            out[idx] = result
        else:
            print(f"  ! failed to parse for q{idx}; leaving blank", file=sys.stderr)
        # write incrementally so a crash doesn't lose progress
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)

    done = sum(1 for o in out if o.get("for") and o.get("against"))
    print(f"Wrote {OUT_PATH}: {done}/{len(questions)} questions have both statements.")
    print("REVIEW the file, then run: python setup_8values.py")


if __name__ == "__main__":
    main()
