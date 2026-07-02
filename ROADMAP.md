# Transcript Analysis — Roadmap

_Last updated: 2026-07-02_

Two analysis products run on top of the sermon-transcript corpus:
1. **Islands** — contiguous high-scoring regions from NLTK n-gram model scoring (literary quote/reference detection).
2. **8values** — an LLM-driven political-values survey of each speaker.

Everything below is grouped by workstream, with status: `[x]` done, `[~]` in progress, `[ ]` not started.

---

## Where things stand (snapshot)

- **Database / infra:** PostgreSQL 17 on the R640, CT 100 (`postgres-server`, 192.168.1.202:5432). Task server + web dashboard co-located on CT 100.
- **Scoring fleet:** model-major distributed clients (`~18.3%` complete — 128.0M of 697.9M `vid × model` pairs). Just fixed three fleet-wide blockers: missing partial indexes, ASCII→UTF-8 client encoding, and broken-connection/worker-loop resilience. Rolling the client fix out to all boxes now.
- **Islands:** pipeline built (`island_worker.py`, `island_task_table`, DB-coordinated claim). Needs reseed + a wrong-image container fixed.
- **8values:** pipeline built (`setup_8values.py`, `embed_transcripts_8values.py`, `generate_probe_statements.py`, `run_8values_survey.py`). Tables + 70 questions + 5 answer options seeded. Per-mode prompts in place (baseline / probe / transcript). Baseline is runnable now.
- **GPU:** NVIDIA T4 passed through to Linux Mint VM 700; Ollama serving on the LAN (`Agent-Sandbox:11434`) with the light model lineup pulled.

---

## Workstream 1 — Finish the scoring run (highest priority; unblocks islands)

- [~] Deploy the client UTF-8 + reconnect fix to **every** worker box (`git pull && ./launch_fleet.sh`). Any box still on the old image keeps stalling at 0.
- [ ] Persist the new indexes in the schema/setup scripts so a rebuild recreates them:
  - `idx_vid_score_model_real` on `vid_score_table (model_id, vid_id) WHERE cardinality(score) > 0`
  - (existing) `idx_vid_transcript_vid_wc`, `idx_vid_score_vid_model`
- [ ] Bake `client_encoding=UTF8` into the island worker and 8values scripts (same locale trap).
- [ ] Progress visibility: hourly snapshot of `pct_complete` + a dashboard card for scoring % and islands %.
- [ ] Decision: **prune the model set?** 9,511 models × 73,380 transcripts = 697.9M pairs is the single biggest lever on total runtime. Confirm all 9,511 models are actually wanted, or trim.
- [ ] Drive scoring to completion (re-baseline the rate once the full fleet is redeployed).

## Workstream 2 — Islands pipeline

- [ ] Fix the island container that was running the wrong image; rebuild `transcript-island-worker`.
- [ ] Reseed `island_task_table` from current scores (`setup_island_tables.py --refresh`) and top up as scoring progresses.
- [ ] Keep island workers running alongside scoring (they're DB-coordinated, independent of the task server).
- [ ] **Island audit tool (precision QA — gates everything).** Interactive reviewer: pull N random islands, show each island's transcript span in context, show what it claims to reference (`island.model_id` → the source work in `model_table`), and let the reviewer mark **yes / no** (real reference or not). Persist judgments and compute precision. Used to (a) audit the current method, (b) tune smoothing window / min length / score threshold, and (c) compare NLTK vs KenLM island quality head-to-head. Likely a dashboard tab or small standalone reviewer writing to an `island_audit` table.

## Workstream 3 — 8values full matrix

Run each mode across the light model lineup (`qwen2.5:7b-instruct`, `qwen2.5:14b`, `mistral-nemo:12b`, `dolphin3:latest`, `llama3.1:8b`, `wizard-vicuna-uncensored:13b`) via the GPU box.

- [x] Per-mode prompts (plain baseline, role-play probe, transcript RAG).
- [ ] **Baseline (no transcript)** — the model's own unprompted answer. Ready to run now.
- [ ] **Probe — succinct for/against** ("I strongly support/oppose X"). Review generated statements → seed → run.
- [ ] **Probe — verbose for/against** (2–4 sentence passages). Review generated statements → seed → run.
- [ ] **Transcript mode** — embed a channel's transcripts (`embed_transcripts_8values.py`), then RAG survey per video.
- Supporting work:
  - [ ] **Variant schema** so verbose and succinct probes coexist and stay comparable (add a `variant` tag to `eightvalues_probe_statement` + `eightvalues_run`; wire seed/build_context/get_probe_statement).
  - [ ] Point `run_8values_survey.py` `OLLAMA_HOST` at the GPU VM by default.
  - [ ] Calibration read-out: per model, baseline lean + probe swing (for vs against) + refusal rate → choose the model(s) to trust for the transcript pass.

## Workstream 4 — Faster scoring (replace NLTK; CPU-fast and/or GPU)

Scoring is CPU-bound because NLTK's pure-Python n-gram scoring is pathologically slow — not because it needs a GPU. We have the **source texts** the models were built from, so we can rebuild the language models with any library; the only constraint is that "islands" (high-scoring regions) stay meaningful.

Key insight: this workload is **n-gram lookups**, which are memory-latency-bound, not compute-bound. A fast CPU n-gram library across the existing fleet often beats a single T4 on lookup-heavy work (GPUs win on compute-bound / neural workloads). Evaluate CPU-fast before committing to GPU.

- [ ] **Option A — KenLM on the CPU fleet (recommended first).** Rebuild LMs from the source texts with KenLM (`lmplz` → quantized binary), query via the Python wrapper. Typically 100–1000× faster per core than NLTK, memory-mapped, parallel across the fleet. Keeps the n-gram method; island thresholds get recalibrated. Biggest win for the least effort — may make "CPU-bound" a non-issue.
- [ ] **Option B — GPU batched n-gram scoring on the T4.** Each model as device-resident arrays (hashed n-gram keys → log-prob + backoff); batch all n-grams across many transcripts, parallel lookups (CuPy/Triton/CUDA). Model-major fits (one model resident at a time). Larger engineering lift; payoff uncertain for a lookup-bound job on one GPU.
- [ ] **Option C — Neural LM on GPU.** Train a small neural LM on the source texts and score on the T4. Best GPU utilization, but changes what "score" means — islands need re-interpretation.
- [ ] Spike + benchmark the chosen path on one model; validate island output looks right; then integrate as the scoring backend.

**Decision (2026-07-02):** Go with **Option A — KenLM**, then benchmark. Acceptance test is not just throughput but **island quality equal-to-or-better than NLTK**, measured with the audit tool (Workstream 2). Island thresholds may be recalibrated to KenLM's score scale; if audited precision is poor under either method, we reconsider the approach entirely. Benchmark plan: rebuild a few models with KenLM from source texts → score a sample → compare per-core throughput vs NLTK **and** audited island precision side by side.

## Workstream 5 — Islands: professional analysis & visualization

- [ ] Nail down the questions the visuals answer (e.g. which speakers/passages quote which sources most; island density per channel; length distributions).
- [ ] Aggregation queries/exports (per model, per video, per channel; island counts, lengths, score profiles).
- [ ] Polished graphs: distributions, heatmaps, per-speaker/per-channel island profiles, top regions with the underlying transcript text.
- [ ] Written report / slide deck of findings.

## Workstream 6 — 8values: professional analysis & visualization

- [ ] The classic 8values compass per speaker / channel (and per model).
- [ ] Model-comparison charts: baseline vs probe swing, cross-model agreement, refusal rates.
- [ ] Confidence/variance across repeats; flag low-agreement questions.
- [ ] Polished graphs + written interpretation.

## Workstream 7 — Infra hardening & record-keeping

- [ ] Fold indexes, `client_encoding`, and other fixes into the setup scripts (reproducible rebuild).
- [ ] DB backup strategy (the 6.3 TB dataset has no quick redo).
- [ ] Runbooks: update `WORKER_SETUP.md`; add an 8values runbook and an islands runbook.
- [ ] Basic monitoring/alerting (fleet stalled, disk, scoring rate).

---

## Suggested sequencing

1. **Now:** finish the fleet rollout + progress visibility (Workstream 1). Everything downstream depends on scores landing.
2. **In parallel (GPU is idle-ish):** 8values baseline → probes → calibration (Workstream 3). Quick wins, independent of the CPU fleet.
3. **Validate the method — build the island audit tool (Workstream 2)** and audit current islands. This is the gate: it tells us whether the whole islands approach is sound before we invest in speeding it up or building final visuals. May drive a threshold change or a method rethink.
4. **KenLM rewrite + benchmark (Workstream 4)** — once the audit tool exists to measure quality, rebuild models with KenLM and compare throughput *and* audited precision against NLTK.
5. **Once scores/islands accumulate and the method is validated:** the analysis & visualization deliverables (Workstreams 5 & 6) — the "professional graphs" outputs.
