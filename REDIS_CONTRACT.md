# Redis Contract (AC scripts ↔ ac-data bridge ↔ Convex)

The Python AC scripts publish telemetry/battle events into Redis Streams. The
Node.js bridge (`ac-data`) consumes those streams and forwards them to Convex
via direct admin mutations.

## Streams

- `ac:events` (configurable via `REDIS_STREAM_KEY`) — runtime telemetry +
  battle results, produced by the Python script and consumed by `ac-data`.
- `ac:config` (configurable via `REDIS_CONFIG_STREAM_KEY`) — `server_cfg.ini`
  snapshots produced by `ac-data` (after polling Convex) and consumed by the
  Python script (`core/redis_config_sync.py`).

Both streams use `XADD` with approximate `MAXLEN` trim
(`REDIS_STREAM_MAXLEN`, default `200000`) so they cannot grow unbounded.

## Stream fields

Each entry contains the same flat fields:

- `event` — logical event type (e.g. `server_status`, `battle_finished`,
  `server_config_snapshot`).
- `eventId` — unique UUID per message (idempotency key).
- `schemaVersion` — schema version string (default `1`).
- `instanceId` — VPS instance (`AC_INSTANCE_ID`).
- `serverName` — readable server name (or `__config__` for snapshot rows).
- `ts` — unix epoch milliseconds.
- `payload` — JSON envelope with the full event.

## Envelope JSON

```json
{
  "eventId": "9ac53f13-e5f3-4c4a-8b0e-48e99295f43b",
  "schemaVersion": "1",
  "event": "battle_finished",
  "serverName": "ProjectD",
  "instanceId": "vps-eu-2",
  "ts": 1778089600000,
  "data": {}
}
```

## Events published into `ac:events`

Generic server lifecycle (`network/event_dispatcher.send_server_event`):

- `player_join`
- `player_leave`
- `lap_completed`
- `server_status` (heartbeat every ~15 s; intentional, used for liveness)
- `server_config_applied` (echo when the Python config consumer applies a
  snapshot to a local `server_cfg.ini` / `entry_list.ini`).

Battle events (`network/event_dispatcher.dispatch_battle_webhook`):

- `battle_update` — produced once when the touge series finishes.
- `battle_finished` — same payload as the closing `battle_update`, emitted
  only when `winnerSteamId` is present so consumers can act on the cleanup
  without ambiguity.

## Events published into `ac:config`

- `server_config_snapshot` — produced by the Node.js bridge after detecting a
  new `version` in Convex. Consumed by the Python config consumer to update
  `core/runtime_config` (server modes) and rewrite local `server_cfg.ini` /
  `entry_list.ini` as needed.

## Worker expectations (Redis → Convex)

- Use `eventId` as dedupe key (idempotency).
- Route by `event`.
- Persist `instanceId`, `serverName`, `ts` next to the domain payload.
- Acknowledge (`XACK`) only after the Convex mutation succeeds.
- Skip / ack-only on `server_config_snapshot` and `server_config_applied` —
  those are consumed by VPS-side workers, not by Convex ingestion.
