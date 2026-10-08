# Streaming guide

How to replay one day of trips through Redpanda and count them with Spark Structured Streaming
([ADR 0012](adr/0012-streaming-replay-with-watermark.md)). Everything runs locally in Docker. Commands are for
Windows Command Prompt.

## Before the first replay

The day must already be in silver locally, for example January 2025:

```
docker compose run --rm spark mobility-lakehouse bronze --month 2025-01
docker compose run --rm spark mobility-lakehouse silver --month 2025-01
```

Start Redpanda and wait until it is healthy:

```
docker compose up -d redpanda
docker compose ps
```

## Replay a day and stream it

1. Write the trips of one day as events (about 650,000 for a weekday):

   ```
   docker compose run --rm spark mobility-lakehouse replay-extract --date 2025-01-15
   ```

2. Send them to the topic `trips`. At the default speed of 600, one day takes about 2.5 minutes. 2% of the
   events are sent up to 90 minutes late:

   ```
   docker compose run --rm spark mobility-lakehouse replay --date 2025-01-15
   ```

3. Count them, then stop:

   ```
   docker compose run --rm spark mobility-lakehouse stream --until-caught-up
   ```

   The summary shows how many events were read and how many were dropped because they were more than 30
   minutes late. Results are in `data/lakehouse/streaming/fhvhv_pickups_15min`.

To watch it live instead, start `stream` without `--until-caught-up` in one window and `replay` in another.
Stop the stream with Ctrl+C.

## Start again from zero

Replaying the same day twice adds the events to the topic twice. To start again, delete the topic, the
results and the checkpoint:

```
docker compose exec redpanda rpk topic delete trips
rmdir /s /q data\lakehouse\streaming data\lakehouse\_checkpoints
```
