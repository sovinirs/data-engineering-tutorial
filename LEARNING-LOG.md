# MarketStream — Learning Log

A running engineering journal for the real-time crypto trade lakehouse build.
Kept for two reasons: (1) interview prep, (2) documented reasoning in the repo itself.

**Project:** Coinbase WebSocket → Kafka/Redpanda → S3 (Iceberg, bronze) → Spark (silver) → Snowflake/dbt (gold), orchestrated by Airflow, provisioned by Terraform.

**Started:** 2026-07-18

---

## How to read this log

- **Q** — checkpoint questions asked during the build
- **My answer** — what I said, unedited
- **Correction / sharpening** — where I was wrong or incomplete, and the right answer
- ⚠️ **Interview flag** — things I got wrong or that are commonly asked; review these first
- **My questions** — clarifications I asked, and the answers

---

## Week 1 — Ingestion path

**Goal:** live crypto trades landing in S3.

**Task map:** `1.LEARN → 2.BUILD → 3.LEARN → 4.BUILD → 5.LEARN → 6.BUILD → 7.BUILD`
Streaming concepts → connect to feed → Kafka/Redpanda → run the broker → storage formats → write to S3 → wire end-to-end.

**Format change as of Task 3:** LEARN tasks are no longer standalone reading assignments — concepts get folded into the following BUILD task and explained inline, right before the code that needs them. Standalone reading caused window-switching/distraction. See `CLAUDE.md` rule 2.

### Status

| # | Task | Type | Status |
|---|------|------|--------|
| 1 | Streaming vs batch, WebSockets | LEARN | ✅ Done (2/3 on checkpoint) |
| 2 | Connect to live feed, print trades | BUILD | ✅ Done |
| 3 | Kafka / Redpanda concepts | LEARN | ✅ Done (folded into Task 4, no standalone checkpoint) |
| 4 | Run the broker, produce to a topic | BUILD | ✅ Done |
| 5 | Storage formats: Parquet & Iceberg | LEARN | ✅ Done (folded into Task 6; 3 reasoning Qs posed, not yet answered verbally — see notes) |
| 6 | Write to S3 bronze | BUILD | 🔨 Core build done, targeting local disk (offline-friendly); crash/dedup test + real S3 swap still open |
| 7 | Wire end-to-end | BUILD | ⬜ Locked |

---

## Task 1 — Streaming vs. batch, and what a WebSocket is (LEARN) ✅

**Why this came first:** Every downstream architecture decision (why a broker, why micro-batches, why dedup is hard) falls out of one fact — in streaming, data arrives *at you* continuously and you don't control the pace. Batch pulls; streaming gets pushed.

### Concepts covered

**Batch vs. streaming**
- Batch: bounded dataset, you decide when to run, you can re-run it.
- Streaming: unbounded, continuous, arrives on the producer's schedule.
- What gets *harder* in streaming: you can't "just re-run it," data can arrive late or out of order, and failure mid-stream leaves partial state.

**WebSocket vs. REST polling**
- REST: you ask, repeatedly, on a timer. Higher latency, wasted calls, and you miss events between polls.
- WebSocket: one persistent connection, server pushes the instant something happens.

**Event time vs. processing time**
- Event time = when the trade actually happened on the exchange.
- Processing time = when my code received/processed it.
- The gap between them is the entire reason out-of-order data exists.

**Delivery semantics** (definitions for now; implemented in Week 2)
- At-most-once, at-least-once, exactly-once.
- Plan: at-least-once delivery + idempotent dedup.

### Checkpoint Q&A

**Q1. Why does a WebSocket beat polling a REST endpoint every second for trade data?**

*My answer:* Because both sender and receiver are connected in real time, resulting in less latency.

*Correction / sharpening:* Correct but incomplete. Lower latency is right, but the bigger reason for **trade** data specifically is that **polling loses events**. If I poll every second and 40 trades happen in that second, I get a snapshot — not all 40. A WebSocket pushes every single match as it occurs, so the stream is complete and gap-free. That completeness is what makes it usable as a source of truth. Bonus: no wasted calls, no rate-limit ceiling.

**Q2. Difference between event time and processing time, and one reason a trade might reach you out of order?**

*My answer:* Event time is the actual timestamp when an event occurs; processing time is when it is received by the system after the event happens and is processed.

*Correction / sharpening:* Definitions correct. Missed the second half — reasons for out-of-order arrival:
- Network path variance between me and the exchange
- **Client reconnect** replaying buffered messages
- **Multiple Kafka partitions or parallel consumers** — messages processed in parallel no longer preserve global order (this is why partition-key choice matters in Task 3)

**Q3. In a Coinbase `match` message, which field for deduplication, and which as event timestamp?**

*My answer:* `maker_order_id` or `taker_order_id` as unique identifier, `time` as timestamp.

*Correction / sharpening:* ⚠️ **Half wrong — and this one matters.**
- `time` as the event timestamp is ✅ correct.
- `maker_order_id` / `taker_order_id` are ❌ **not unique per trade.** A single large resting order can be matched against dozens of incoming orders, so the same `maker_order_id` appears across many different `match` messages. Deduplicating on it would **silently drop real trades** — a data-loss bug that wouldn't surface until volume numbers looked mysteriously low.
- Correct dedup key: **`trade_id`**, unique per executed trade per product. Effectively `(product_id, trade_id)`.

**Also noted: the `sequence` field.** Monotonically increasing counter per product. Not used for dedup, but it's how gaps get *detected* — seeing sequence 1004 then 1006 means a message was dropped. Free data-quality check in Week 2, and a strong interview talking point.

### Shape of a `match` message

```
type, trade_id, sequence, maker_order_id, taker_order_id,
time, product_id, size, price, side
```

### ⚠️ Interview flags from Task 1

1. **Uniqueness of an ID is not the same as it being present on every record.** Always ask "can this value repeat across rows?" before making it a dedup key. Order IDs repeat; trade IDs don't.
2. **"Why streaming over polling?"** — lead with *completeness*, not just latency.
3. **Out-of-order causes** — network variance, reconnect replay, parallel partitions/consumers.
4. **Gap detection via a sequence number** is a cheap, high-signal data-quality control.

---

## Task 2 — Connect to the live feed, print your first real trades (BUILD) ✅

**Why this task:** Before any Kafka, S3, or Iceberg, prove the data is real and *see its actual shape*. Every schema decision downstream comes from what's observed here.

### Requirements

Build `producer/coinbase_client.py` that:

1. Connects to `wss://ws-feed.exchange.coinbase.com` (no auth, no API key).
2. Sends a subscribe message on connect for the `matches` channel on **3 products** — `BTC-USD`, `ETH-USD`, `SOL-USD`. Multiple products make ordering non-trivial and give something real to partition on later.
3. Prints each incoming message.
4. **Handles reconnects** — on drop, wait and reconnect with exponential backoff rather than crashing. Long-running connections *will* drop; a producer that dies at 3am doesn't work.
5. Logs a heartbeat every N messages (e.g. "processed 1000 messages, 340 msg/sec") to observe throughput.

### Hints given

- `websockets` (async, closer to professional code) or `websocket-client` — either fine.
- Subscribe payload is JSON: `type: "subscribe"`, a `product_ids` list, a `channels` list.
- Non-`match` messages will arrive too (`subscriptions`, `heartbeat`, errors). Filter by `type` — and *notice* that handling mixed message types on one stream is a real concern.
- Keep config (product list, URL) as constants at the top, not scattered inline. Moves to config in Week 2.

### Definition of done

- [x] Script written
- [x] 2–3 real `match` messages captured and pasted
- [x] Observed throughput (messages/sec across the three products)

> The throughput number is the first metric for the README — it's the number a recruiter's eye actually lands on.

### Notes from the build

First pass of `producer/coinbase_client.py` had the connect/subscribe/print/filter logic right (requirements 1–3) but two real bugs in the reconnect and heartbeat logic (requirements 4–5), found by walking the code line by line rather than a one-shot review:

**Bug 1 — `UnboundLocalError` on first-connect failure.** `delay = INITIAL_DELAY` was only assigned *inside* the `async with websockets.connect(...)` block — i.e., only after a connection already succeeded. If the very first connection attempt fails before ever entering that block, the `except` clause references `delay` before it's ever been assigned, and the "resilient" producer crashes on exactly the failure mode it was built to survive.

- Fix: assign `delay = INITIAL_DELAY` once, above the `while True:` loop, so it exists regardless of whether `websockets.connect` ever succeeds.
- Design decision made alongside the fix: on a *successful* reconnect, `delay` resets back to `INITIAL_DELAY` (kept the existing reset line inside the connected block) rather than staying at whatever it had climbed to. Reasoning: a connection that stayed up for hours before dropping is a different failure mode than one that can't connect at all, back-to-back — it shouldn't inherit backoff from an unrelated earlier outage.

**Bug 2 — heartbeat logged a count, not a rate.** Original code printed `"{COUNTER} messages processed"` — volume, not throughput. Requirement 5 asked for msg/sec specifically, since that's the number that goes in the README.

- Fix: capture `start_time = time.time()` once, before the `async for` loop (capturing it inside the loop would reset it every message, making elapsed time always ~0). At each heartbeat, compute `COUNTER / (time.time() - start_time)`.
- Caveat to remember: this is a **cumulative average since the connection opened**, not a rolling/instantaneous rate — it smooths out short bursts and resets only on reconnect. Fine for a first pass; would need a windowed calculation (e.g., count-since-last-heartbeat / time-since-last-heartbeat) to show current rate instead of lifetime average.

**Real-data confirmation of the Task 1 dedup lesson, twice over.** Captured trades showed the same `maker_order_id` repeated across multiple `match` messages (one resting order matched against several takers) — and separately, a SOL-USD burst where the same `taker_order_id` appeared against six different `maker_order_id`s at the identical timestamp (one incoming order sweeping six resting orders instantly). Confirms `trade_id` is the only safe dedup key — either side of the trade can repeat, not just the maker.

**Observed throughput:** 4.85 msg/sec and 5.36 msg/sec (two heartbeat readings, cumulative average per connection, BTC-USD/ETH-USD/SOL-USD combined). Short observation window — not a claim of sustained throughput, just what was measured.

### My questions

None asked this task — mostly a code walkthrough. Did push back partway through that reviewing all issues as an upfront list and asking for a re-paste was slow and expected too much first-pass robustness; switched to going through the code piece by piece instead, which is reflected in how this task's notes are structured above.

---

## Task 3+4 — Kafka/Redpanda concepts, folded into running the broker and producing to a topic (LEARN + BUILD) ✅

**Why folded together:** Format changed here (see note above) — concepts explained inline as the build needed them, rather than as a separate reading task.

### Concepts covered

**Why a broker at all (not producer → S3/monitor/Spark directly).** Reasoned through from two questions:
- *If a downstream consumer (e.g. an S3-writer) is slow, what happens to the producer?* A synchronous direct write inside the message loop **couples producer speed to the slowest consumer's speed** — it's not just "add a connection check," a slow/down consumer stalls or crashes the whole ingestion path, since the producer can't keep reading the websocket while blocked on that write.
- *What changes in the producer every time a new consumer is added?* New code, every time — the producer becomes a chokepoint that has to know about every downstream system.
- A broker fixes both: **decoupling** (producer writes once, broker holds it, a slow consumer only stalls itself) and **fan-out** (any number of consumers read independently, producer code never changes).

**Topics and partitions.** A partition is an ordered, append-only log; Kafka/Redpanda guarantees order *within* a partition, never across partitions. Two consequences:
- **Partition count** caps parallelism (a topic with 3 partitions supports at most 3 actively-reading consumers in a group).
- **Partition key** decides which partition a message lands in (`hash(key) % partition_count`), and therefore what's ordered relative to what.
- Applied to this project: keying by `product_id` (3 partitions, one per product) preserves per-product order — which is exactly what the `sequence`-gap detection from Task 1 depends on to be valid (scattering one product's trades across partitions would produce false gaps). Cross-product ordering is lost, but nothing in this project (dedup is per-trade, gap detection is per-product) ever needs it, so the tradeoff is free here.
- Follow-up worth remembering: if cross-product ordering were ever needed, it's not automatic — you'd have to buffer and reorder by the `time` field downstream, with a watermark deciding how long to wait for stragglers. That's exactly the **out-of-order/late-arrival handling** `CLAUDE.md` calls out as a first-class requirement — it isn't free, and it isn't given by the broker.

**Kafka vs. Redpanda.** Same wire protocol/API (topics, partitions, consumer groups, client libraries all identical) — the difference is the implementation underneath. Kafka: JVM-based, historically ZooKeeper-dependent, real memory/GC overhead. Redpanda: single C++ binary, thread-per-core, no JVM, never needed ZooKeeper — lighter footprint, faster startup, better fit for a laptop dev setup. Framing for a resume/interview: *"built on Kafka's protocol and semantics, ran on Redpanda locally for resource efficiency"* — accurate, not something to downplay or hide.

**Deferred, not yet covered:** consumer groups/offsets, and delivery semantics *in practice* (acks/idempotent-producer config, as opposed to the Task 1 definitions). Neither is needed yet since Task 4 only built the producer side — no consumer exists yet. These will get folded into whichever task builds the first real consumer (writing to S3, Task 6/7).

### Decision: broker hosting

Asked whether to run the broker on AWS now (MSK or self-managed EC2) instead of locally, partly to build AWS skills for the resume. Decided **local Redpanda in Docker for now; AWS deferred to a dedicated later task.** Reasoning: MSK bills continuously (or per-usage for Serverless) even when idle, which cuts against `CLAUDE.md`'s "cost control" requirement for a project that isn't being worked on 24/7; the project already has substantial AWS surface coming later (S3, likely Glue/Iceberg catalog, IAM, Terraform), so the broker specifically doesn't have to be the thing proving AWS competency right now.

### Requirements (Task 4)

1. Run Redpanda locally via Docker (single-node, no cluster needed).
2. Create a topic `trades` with **3 partitions**.
3. Modify `producer/coinbase_client.py` to produce each `match` message to `trades`, **keyed by `product_id`**, using a real Kafka client library (`confluent-kafka` chosen).
4. Delivery confirmation via callback, not fire-and-forget.

### Definition of done

- [x] Redpanda running locally in Docker, `trades` topic created with 3 partitions
- [x] Producer sends match messages to `trades`, keyed by `product_id`
- [x] Delivery confirmed both via `delivery_report` callback output and `rpk topic consume`

### Notes from the build

**Docker config started as a full Redpanda quickstart example, not something written for this task** — a docker-compose folder appeared with SASL auth, Schema Registry, Redpanda Console, and an entirely unrelated WASM "data transforms" tutorial (a `transactions` topic/schema, Go transform code) bundled in, plus a stray `.claude/settings.local.json` that came along with it. Recognizably Redpanda's own official example repo content, not authored for this project. Per `CLAUDE.md`'s "own and understand this code" rule, stripped it down to just what Task 4 needs: single broker, no auth, no schema registry, no console, `trades` topic only. Deleted the unrelated transform/profiles example files and the stray `.claude` folder entirely.

**Internal vs. external broker addresses.** `--kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092` exists because a process *inside* Docker (on the `redpanda_network` bridge) reaches the broker by container name (`redpanda-0:9092`), while a process on the host machine (the Python producer, running natively, not containerized) can't resolve that container name — it needs the externally-published port (`localhost:19092`). `localhost` inside a container is always that container's own loopback, never another container or the host — this is the general rule, not just a Redpanda quirk.

**Producer bugs, found by walking the code piece by piece (consistent with the Task 2 approach):**

1. **`producer.flush()` called after every single message, inside the async loop.** `flush()` is a *blocking* call that waits for all outstanding deliveries — calling it per-message serializes every send to a full round-trip, defeating the entire point of an async producer, and (worse) blocks the asyncio event loop itself while waiting, the same category of problem as a slow synchronous S3 write blocking the websocket loop (see broker-justification reasoning above). Fix: `producer.poll(0)` (non-blocking) after each `produce()`, to service delivery callbacks without stalling; `flush()` reserved for actual shutdown only.
2. **Two separate regressions introduced while fixing the above, in the same edit:**
   - `produce()`/`poll()` got nested *inside* the `if COUNTER % N == 0:` heartbeat check instead of directly under the `if type == "match":` check — meaning only 1-in-10 match messages were actually being produced, silently dropping the other 9. "Is this a trade" and "is it time to log a heartbeat" are independent conditions and shouldn't be nested inside each other.
   - The plain `print(data)` per match was dropped during the same restructuring. Decided this one's fine to leave out — `delivery_report`'s per-message "delivered to trades [partition]" output is stronger confirmation than a local print ever was (it confirms actual Kafka delivery, not just local processing).
3. **Flush-on-shutdown took three attempts to land in the right place**, each one the same underlying mistake in a new spot: code placed immediately after a call, assuming that call would return normally, when the call was exactly the thing expected to raise/interrupt.
   - First attempt: `producer.flush()` placed after the `async for` loop inside `coinbase_stream()` — unreachable, because that loop only ever exits via a caught `ConnectionClosed`/`OSError` exception, which jumps straight to the `except` block, skipping past it.
   - Second attempt: moved `producer` to module scope (necessary, so `__main__` could reach it) and placed `flush()` right after `asyncio.run(coinbase_stream())` inside `__main__`'s `try:` — still unreachable, since `coinbase_stream()` runs forever and the *only* way `asyncio.run()` returns is via `KeyboardInterrupt`, which skips past that line straight to `except KeyboardInterrupt:`.
   - Landed: `producer.flush()` placed after the `try/except` block entirely (still inside `if __name__ == "__main__":`), so it runs regardless of whether the `except` caught something — the actual shutdown path.

**Also produced to Kafka along the way: real confirmation of correct filtering.** `rpk topic consume` showed old `subscriptions` (no key) and `last_match` (has a key — `last_match` carries `product_id` too, unlike `subscriptions`) messages sitting at the earliest offsets, left over from before the type-filter existed. Topic retention meant they weren't purged when the code was fixed — a reminder that verifying "the code is right" and "the topic is clean" are two different checks; recreating the topic is the way to get a clean sample later if needed.

### My questions

Asked whether AWS (MSK or self-managed EC2) should host the broker now instead of Docker locally, for AWS skill-building and the resume. Answered above under "Decision: broker hosting" — local now, AWS as a deliberate later task.

---

## Task 5+6 — Storage formats (Parquet & Iceberg) + Kafka consumer writing to local Iceberg bronze (LEARN + BUILD) 🔨

**Why folded together:** same format change as Task 3+4 — concepts explained inline against the real build rather than as standalone reading.

### Concepts covered (Task 5)

**Why not raw JSON as the storage format:**
- **Columnar pruning.** Parquet stores each column contiguously on disk; a query needing `avg(price) by product_id` reads only those two columns' bytes. JSON interleaves every field per record, so extracting even one field means parsing the whole object, every record.
- **Type safety.** `price`/`size` arrive as JSON strings (`"64728.45"`). JSON enforces no type — every downstream reader has to independently cast, with no shared contract and no guaranteed consistency across tools. Parquet bakes the type in once, at write time.
- **Small-files problem, in concrete numbers.** At the ~5 msg/sec measured in Task 2, one JSON file per trade would mean `5 × 3600 ≈ 18,000` files/hour, ~432,000/day — first pass at this arithmetic used division instead of multiplication and had to be corrected.

**Why Iceberg on top of Parquet — three reasoning scenarios, revisited after being skipped the first time:**

1. **Schema change.** *My answer: didn't know.* A naive "read every Parquet file under this prefix" approach breaks because each Parquet file is self-describing (its own schema in the file footer) but nothing ties them into one coherent table schema across files — mismatched files either error out, or (Spark with schema-merging) require reading every file's footer to reconcile, and a rename is indistinguishable from "column vanished, unrelated column appeared." **Iceberg's fix:** every column has a permanent **field ID** (see interview flag #17), tracked centrally in versioned table metadata. Add a column → new schema version, old files are known to be missing that field ID, readers return null for it. Rename → same field ID, relabeled, old data untouched. The engine asks the catalog for the current schema once, instead of inferring it file-by-file.

2. **Partition pruning.** *My answer: didn't know.* Without a table format, "know which files to look at" means calling S3's LIST API to walk the folder structure before reading any actual data — at scale, thousands of LIST calls per query, each a real network round trip and API cost, paid every time regardless of query selectivity. **Iceberg's fix:** a compact metadata layer (manifest files) records exactly which data files belong to which partition values, plus per-file min/max column stats, so the engine reads a handful of small manifest files instead of listing S3 — and can skip files *within* a matching partition too, using the stats. This is the literal mechanism behind `CLAUDE.md`'s "cost control (Iceberg partition pruning)" line.

3. **Crash mid-write.** Instead of answering in the abstract, decided to test it empirically once the consumer existed — see the crash-test plan in Task 6's open items below.

⚠️ Both explanations above were given directly rather than derived independently — logged honestly per this journal's own rule (record what I got wrong or didn't know, not a cleaned-up version). Worth being able to re-derive/explain 1 and 2 unprompted before an interview, not just recognize them read back.

### Offline prep (this build happened around a 2-hour flight with no connectivity)

- Installed `pyiceberg[pyarrow,sql-sqlite]` while still online — pure Python, no JVM, and supports a fully local catalog (SQLite metadata DB + local `file://` warehouse dir), so the real Task 6 logic could be built and tested with zero network access, then repointed at S3 later.
- Verified the local catalog pattern with a throwaway smoke test (create table, write rows, read back) before losing connectivity — worked, then deleted as scratch.
- Saved `pyiceberg` and `confluent-kafka` Consumer API references, plus a self-contained task brief, to `docs/reference/` — necessary because **I (the assistant) am cloud-based and equally unreachable without internet**, not just the AWS parts of the pipeline. Deleted by choice once back online in favor of the live official docs.

### Requirements / decisions made (Task 6, offline-friendly variant — local disk instead of S3 for now)

Build `consumer/trades_to_bronze.py`:
1. `confluent_kafka.Consumer` reads `trades`, batches messages (not one Iceberg write per message — same small-files reasoning as above, just one layer down).
2. Batch cast to match an explicit Iceberg schema, then appended to a local Iceberg table.
3. Kafka offset committed **only after** a successful Iceberg append — at-least-once, matching the Task 1 plan.

**Schema decisions:** `price`/`size` as `DecimalType(18, 8)` — chosen over `DoubleType` specifically to avoid float rounding on money, direct continuation of Task 5's type-safety point. `trade_id`/`sequence` as `LongType` (64-bit) — real observed values like `sequence: 132876007876` exceed a 32-bit int's ~2.1B ceiling. `time` as `TimestampType`. Everything else `StringType`.

**Partitioning:** `PartitionSpec` with **both** a `DayTransform()` on `time` and an `IdentityTransform()` on `product_id` — queries will filter by date range and likely by product, so both get pruning.

### Definition of done

- [x] Consumer reads `trades`, batches, casts types, writes to a local Iceberg `bronze.trades` table
- [x] Offset committed only after a successful append
- [x] Verified with real data: 1,400 rows landed with correct types (`decimal128(18,8)`, `timestamp[us]`, `int64`), confirmed via a direct PyArrow scan, not just "no errors"
- [x] Runs correctly regardless of working directory (tested from two different launch locations)
- [ ] Crash-mid-batch duplicate test (kill after append, before commit, restart, confirm a duplicate `trade_id`) — reasoned through, **not yet actually run**
- [ ] Task 5's three Iceberg-justification questions answered explicitly in words
- [ ] Real S3 target (currently local disk only)

### Notes from the build — bugs found, roughly in the order hit

1. **`poll()` and `consume()` called together in the same loop iteration** — redundant, not a real pattern. Settled on `consume(num_messages, timeout)` alone. Correction along the way: initial understanding of `poll(timeout=...)` as "checks every X seconds" was imprecise — it's a single call that blocks *up to* X seconds and returns as soon as one message is available, not a fixed interval. Also: `consume()` has no default timeout and can block indefinitely if one isn't passed.
2. **`consume()`'s empty-batch signal is `[]`, not `None`** (different from `poll()`) — and errors can appear as individual entries mixed into the returned list, requiring a per-item `.error()` check rather than one check per call.
3. **`listItem.value` missing `()`** — printed the bound method object instead of the actual bytes. Fixed to `.value().decode()` + `json.loads()`.
4. **Duplicate `field_id=1`** on two different `NestedField`s (`type` and `trade_id`). Iceberg field IDs are the stable identity of a column across schema evolution — not the name or position — and must be unique. Renumbered 1–10.
5. **`DecimalType(precision=2)` missing the required `scale` argument** — would crash at `Schema()` construction (module-level code, so at import time, before the script even starts). Fixed to `DecimalType(precision=18, scale=8)`, sized against actually-observed price/size ranges, not guessed.
6. **`batch.append()` called with no argument** — real `TypeError` the moment a message arrived; the parsed value was computed and silently discarded. Regressed at the same time: the per-message print disappeared too. Both restored.
7. **Catalog config pointed at infrastructure that didn't exist** — a `type="rest"` catalog at `http://localhost:8181` (nothing running there) and a placeholder bucket `s3://my-iceberg-warehouse/data`, both copied from official docs examples rather than the working local-`SqlCatalog` pattern already proven in the smoke test. Real friction here: the smoke-test code itself had already been deleted after running it, so "same as the smoke test" wasn't something to actually reference — had to be given the concrete snippet directly instead of pointed at something no longer inspectable. ⚠️ Lesson for how this log-keeper (me) should operate: don't gesture at deleted/unreachable prior work as if it's a sufficient pointer.
8. **Namespace/table named `analytics`/`user_events`** — also copy-pasted from a generic docs example, unrelated to this project. Renamed to `bronze`/`trades`.
9. **`create_table` instead of `create_table_if_not_exists`** — would raise `TableAlreadyExistsError` on every restart after the first. Separately, `create_namespace_if_not_exists` was called with `schema=`/`partition_spec=` kwargs it doesn't accept — those belong to table creation, not namespace creation (a namespace is just an organizational container, nothing else). Split into two correct calls.
10. **Type casting at the Kafka→Iceberg boundary:** `price`/`size` cast via `Decimal(string)` directly — explicitly *not* through `float()` first, since a float has already lost the precision `DecimalType` exists to preserve. `time` needed its trailing `"Z"` swapped for `"+00:00"` before `datetime.fromisoformat()` — **Python 3.9** (this project's actual interpreter) doesn't parse a trailing `Z`; that support only arrived in Python 3.11.
11. **`table.schema().as_arrow()`** used to derive the PyArrow schema for `pa.Table.from_pylist()`, instead of hand-declaring a second schema that could silently drift from the Iceberg one — one source of truth.
12. **SQLite `unable to open database file`** — the catalog's `uri`/`warehouse` paths were relative to whatever directory the script happened to be launched from, not the project root. Fixed by anchoring both to the script's own file location (`os.path.dirname(os.path.abspath(__file__))`), so it's invocation-directory-independent. Verified by actually running it from a different working directory, not just reasoning about it.
13. **`trades` topic silently regressed from 3 partitions to 1** at some point after Task 4. Root cause confirmed via the `createtopic` helper container's own logs: `TOPIC_ALREADY_EXISTS`, three times. `depends_on: condition: service_healthy` in `docker-compose.yml` only orders containers *within that compose file* — it has no coordination power over an externally-launched host process (the producer script). On a stack restart, the producer almost certainly connected first and Redpanda auto-created `trades` with the default 1 partition before the dedicated `createtopic` container's explicit `--partitions 3` got to run. Fixed by deleting and recreating the topic — this dropped 153 backlog messages still unconsumed in Kafka (fine, dev-only) but did **not** affect the 1,400 rows already durably written to the Iceberg table, since those two are independent once written.

### My questions

No formal clarification questions distinct from the bug narrative above. One real process moment worth recording: pushed back hard when told to fix a catalog config "the same shape as the smoke test" — the smoke test code had already been deleted, so that wasn't an actionable pointer, only a source of frustration. Landed on: for mechanical/infrastructure details (exact API shapes, config values), give the concrete answer directly rather than gesturing at something not actually inspectable — reserve the "figure it out yourself" approach for genuine design/reasoning decisions, not API trivia.

---

## Running list of interview flags

| # | Flag | Source |
|---|------|--------|
| 1 | Uniqueness ≠ presence: verify an ID can't repeat before using it as a dedup key | Task 1 |
| 2 | WebSocket over polling → argue *completeness* first, latency second | Task 1 |
| 3 | Out-of-order causes: network variance, reconnect replay, parallel partitions | Task 1 |
| 4 | Sequence-number gap detection as a cheap data-quality control | Task 1 |
| 5 | Initialize retry/backoff state *before* the connect attempt, not inside a block only reached after success — otherwise the first-ever failure crashes with an unbound variable | Task 2 |
| 6 | Backoff should reset to the initial delay on a successful reconnect, not persist — a long-lived connection dropping isn't the same failure mode as one that can't connect at all | Task 2 |
| 7 | A running average since connection start is not the same as instantaneous throughput — be able to say which one a given metric actually measures | Task 2 |
| 8 | Either side of a trade can repeat (`maker_order_id` *and* `taker_order_id`), reinforcing `trade_id` as the only safe dedup key | Task 2 |
| 9 | Why a broker at all: decoupling (slow consumer stalls itself, not the producer) + fan-out (new consumers don't touch producer code) | Task 3+4 |
| 10 | Partition key choice = ordering guarantee choice; key by whatever dimension actually needs order preserved (here, `product_id`, because `sequence`-gap detection needs it) | Task 3+4 |
| 11 | Kafka and Redpanda share a wire protocol; the difference is the runtime (JVM+ZooKeeper vs. single C++ binary) — be ready to state this plainly rather than downplay it | Task 3+4 |
| 12 | A blocking call (`flush()`) inside an async loop stalls the event loop the same way a slow synchronous write would — same failure category, different disguise | Task 3+4 |
| 13 | Code placed right after a call is not "after" it if that call is the thing that raises/interrupts — check what actually happens on the exception path, not just the happy path | Task 3+4 |
| 14 | Docker Compose's `depends_on: condition: service_healthy` only orders containers *within the same compose file* — it can't coordinate with an externally-launched host process, which can race ahead and silently change infrastructure state (e.g. auto-creating a topic with default settings before an explicit create step runs) | Task 5+6 |
| 15 | Python 3.9's `datetime.fromisoformat()` doesn't parse a trailing `Z` — that support only arrived in Python 3.11. Know your actual interpreter's behavior, don't assume uniform ISO 8601 support | Task 5+6 |
| 16 | Convert `Decimal` from the original string, never through `float()` first — float has already lost the precision `Decimal`/`DecimalType` exist to preserve | Task 5+6 |
| 17 | Iceberg field IDs are a column's stable identity across schema evolution (not name or position) and must be unique within a schema — this is *why* renaming a column doesn't break downstream readers | Task 5+6 |
| 18 | Anchor file/config paths to the script's own location (`os.path.abspath(__file__)`), not the current working directory, for anything that must work regardless of how/where it's invoked | Task 5+6 |

---

## Metrics for the README

| Metric | Value | Captured |
|--------|-------|----------|
| Ingest throughput (msg/sec) | ~4.9–5.4 msg/sec (BTC/ETH/SOL combined, cumulative avg per connection, short observation window) | Task 2 |
| End-to-end latency | _TBD_ | Week 3 |
| Data quality test coverage | _TBD_ | Week 2 |
| Monthly warehouse cost | _TBD_ | Week 4 |
