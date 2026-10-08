# 0012. Streaming: replay through Redpanda, 30-minute watermark, reconciled with batch

- Status: Accepted
- Date: 2026-10-08
- Decider: Nafiul Hadi Saputra

## Context

The batch lakehouse processes one month at a time. Many data teams also run streaming pipelines, and the
hard part of streaming is not reading messages but deciding what to do with events that arrive late or out
of order. The TLC data has no live feed: trips are published monthly, about two months late.

The stream must be repeatable, so that its result can be checked against something known to be right.

## Decision

- **Replay one day of silver trips as events.** `replay-extract` writes the trips that started on one day as
  event files. `replay` sends them to a Redpanda topic in pickup-time order, faster than real time.
- **Inject late and out-of-order events on purpose.** By default 2% of the events are sent between 1 and 90
  minutes late. The plan uses a fixed seed, so the same command always sends the same events in the same
  order.
- **Redpanda** is the broker: Kafka-compatible, one container, no ZooKeeper. The producer is idempotent, so a
  retry does not write an event twice.
- **Spark Structured Streaming** counts trips per pickup zone and 15-minute window, the same engine and API
  as on Databricks.
- **Watermark of 30 minutes.** An event up to 30 minutes behind the latest pickup time seen is still counted
  in its window. An older event is dropped, and the number of dropped rows is taken from Spark's progress
  metrics and reported, so that every missing trip is explained.
- **Update mode, merged into Delta.** Each micro-batch upserts the changed windows by (window start, zone).
  A batch that runs again after a failure writes the same rows. The checkpoint stores offsets, watermark and
  window state, so a restarted stream continues where it stopped.
- **Two ways to run:** `--until-caught-up` processes what is in the topic and stops (repeatable, used for
  checks); without it, the stream runs live while the replay is sending.
- **Correctness is proven against batch.** For the replayed day, trips per zone and hour in the stream must
  equal the batch gold table `hourly_pickup_zones`, minus the events the stream dropped as too late.
  `mobility-lakehouse reconcile` checks this and writes a report to `docs/reports`. Every stream run is
  recorded with the late rows Spark dropped, so the check uses Spark's own numbers.

## Consequences

- The stream is tested without Redpanda: the same query reads JSON files in a Spark test, which checks that a
  late event inside the watermark is counted, an older one is dropped and counted as dropped, and a run with
  no new events changes nothing.
- Streaming runs locally in Docker. Databricks Free Edition cannot reach a broker on a laptop, so the
  Databricks part of the project stays batch.
- With 90-minute maximum delays and a 30-minute watermark, some events are dropped by design. This is the
  trade-off a watermark makes: results become final after 30 minutes instead of never.

## Alternatives considered

- **A Python consumer.** Simpler to read, but windows, watermarks and state would be written by hand, and it
  shows nothing about Spark streaming. Rejected.
- **No watermark.** No event is ever dropped, but window state grows without limit and no result is ever
  final. Rejected.
- **Append output mode.** Writes each window once, when the watermark passes it, but the last windows of a
  replay are only written after newer events arrive. Update mode with a merge shows results as they change.
  Rejected.
- **Apache Kafka.** The reference implementation, but needs more containers and memory on a laptop than
  Redpanda for the same client API. Rejected for local development.
