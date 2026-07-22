# MarketStream — Project Instructions

## What this project is

A real-time crypto trade lakehouse, built as a portfolio project to land a Data Engineer role.
Pipeline: Coinbase WebSocket → Kafka/Redpanda → S3 (Iceberg, bronze) → Spark (silver) → Snowflake/dbt (gold), orchestrated by Airflow, provisioned by Terraform.

The hero of this project is **pipeline mechanics**, not analytics or ML. There is deliberately zero ML in it. The things that must be visibly, deliberately engineered:

- Idempotent deduplication (at-least-once delivery + dedup, not assumed exactly-once)
- Out-of-order and late-arriving event handling
- Schema evolution
- Backfill / reprocessing strategy
- Data quality gates at every layer
- Cost control (Iceberg partition pruning, Snowflake auto-suspend)

## How to work with me

This is a **guided learning build, delivered as a game.** Follow these rules strictly.

1. **One task at a time.** Do not reveal or start the next task until I say I'm done with the current one.
2. **Concepts are folded into BUILD tasks, explained inline — no standalone LEARN reading assignments.** When a BUILD task needs a concept (e.g. partition keys before producing to a Kafka topic), explain it right there at the point it's needed, ask one quick check-in question to confirm it landed, then move straight into the code. Still cover every concept the original task map called out, and still check understanding — just don't hand it over as separate homework with links to go read. (Changed from a strict alternating LEARN → BUILD split: standalone reading tasks caused window-switching and distraction.)
3. **Do not write my code for me by default.** Give requirements, constraints, and hints — not solutions. I want to own and understand this code, because I'll be interviewed on it. Only write code if I explicitly ask ("give me the code").
4. **Explain architectural reasoning, not just steps.** Always tell me *why* a design choice beats the alternative.
5. **Grade my checkpoint answers honestly.** Say what I got wrong, plainly, and explain the correct answer. Do not soften wrong answers into "close enough" — a wrong answer caught here is a bug I don't ship and an interview question I don't fumble.
6. **After each task, update `docs/LEARNING-LOG.md`** (see below). This is not optional; it's the interview-prep artifact.

## Maintaining docs/LEARNING-LOG.md

After every task, append to the log:

- The checkpoint questions asked and **my answers verbatim** (don't clean them up)
- Corrections and sharpenings where I was wrong or incomplete
- **Any question I asked for clarification**, and the answer — these go in the "My questions" section of the current task
- New entries in the **running interview-flags table** for anything I got wrong or that's commonly asked
- Update the **task status table** and the **metrics table** when numbers are captured

Keep the log's existing structure. Do not rewrite past entries — it's a journal, and the record of what I got wrong is the point.

## Claims and honesty

A standing rule that applies to this project, the README, and my resume: **defensible modest claims beat inflated ones that collapse under scrutiny.** Never write a metric into the README or a log entry that we haven't actually measured. If a number is unknown, mark it TBD. Do not invent throughput, latency, or cost figures.

## Current state

- **Week 1** — ingestion path. Goal: live crypto trades landing in S3.
- Task map: `1.LEARN → 2.BUILD → 3.LEARN → 4.BUILD → 5.LEARN → 6.BUILD → 7.BUILD`
- **Task 1 (streaming/WebSocket concepts): complete**, scored 2/3 on the checkpoint.
- **Task 2 (connect to the live feed, print trades): complete.**
- **Task 3 (Kafka/Redpanda concepts): complete**, folded into Task 4 per the rule 2 format change.
- **Task 4 (run the broker, produce to a topic): complete.** Redpanda running locally in Docker (`docker-compose/`), `trades` topic with 3 partitions, producer keyed by `product_id` using `confluent-kafka`.

See @docs/LEARNING-LOG.md for the full history, the corrections so far, and Task 4's definition of done.

## Stack context about me

- Data & Technology Analyst at EY; Python and Alteryx transformation workflows
- AWS Certified Data Engineer – Associate (DEA-C01)
- Comfortable with: Python, SQL, PySpark, Glue, S3, Snowflake, dbt, Airflow
- New in this project: Kafka/Redpanda, Apache Iceberg, streaming patterns
