# Nox · Ingestor Replayer

[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](./LICENSE) [![Docs](https://img.shields.io/badge/docs-nox--protocol-purple)](https://docs.iex.ec) [![Discord](https://img.shields.io/badge/chat-Discord-5865F2)](https://discord.com/invite/5TewNUnJHN) [![Ship](https://img.shields.io/github/v/tag/iExec-Nox/nox-ingestor-replayer?label=ship)](https://github.com/iExec-Nox/nox-ingestor-replayer/releases)

> On-demand HTTP API that republishes a historical range of NoxCompute blocks to NATS JetStream.

> [!NOTE]
> This is not the continuous scanner. `nox-ingestor` follows the chain head and publishes as blocks arrive; this service does nothing until an authenticated `POST /replay` asks it for a bounded block range. It keeps no cursor and no persistent state of any kind.

## Table of Contents

- [Nox · Ingestor Replayer](#nox--ingestor-replayer)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Prerequisites](#prerequisites)
  - [Getting Started](#getting-started)
  - [Environment Variables](#environment-variables)
  - [HTTP Endpoints](#http-endpoints)
    - [Service Endpoints](#service-endpoints)
      - [`GET /`](#get-)
      - [`GET /health`](#get-health)
      - [`GET /metrics`](#get-metrics)
        - [NATS](#nats)
        - [Replay](#replay)
        - [Build](#build)
    - [Replay Endpoints](#replay-endpoints)
      - [`POST /replay`](#post-replay)
      - [`GET /replay/status`](#get-replaystatus)
  - [NATS Message Format](#nats-message-format)
    - [Subject](#subject)
    - [Payload](#payload)
    - [Event Types](#event-types)
  - [Related Repositories](#related-repositories)
  - [License](#license)

---

## Overview

The replayer is the gap-recovery tool of the Nox Protocol ingestion path. When the JetStream stream is missing NoxCompute events for a block range, an operator posts that range to the replayer. It re-reads the range from that chain's RPC endpoint, rebuilds one message per transaction in exactly the format the live ingestor produces, and republishes each one to the same subject. Downstream consumers cannot distinguish a replayed message from a live one, and are not meant to.

> [!IMPORTANT]
> Because a replayed message is indistinguishable from a live one, `blockNumber` **is not monotonic on the stream**: replaying an old range puts low block numbers after higher ones that are already stored. A consumer that keeps a `blockNumber` high-watermark and drops anything below it will silently discard every replayed message. The replay reports success on `/metrics` and changes nothing downstream. Consumers must key on `(chainId, transactionHash)` instead.

**Request-driven replay (`POST /replay` → NATS):** the block range comes from the request body on every call. There is no cursor and no state file. The job reads the range in batches sized by the chain's `batch_size`: one `eth_getLogs` per batch, whose transactions are all materialized in memory before the first publish, so a wide `batch_size` raises peak memory. Publishes are then serialized within the batch, each transaction is published and acknowledged before the next one is published, with no pipelining. Each configured chain gets its own pipeline (an RPC reader plus a NATS publisher), so replays on different chains proceed independently, though all pipelines share a single NATS connection and JetStream context.

**Concurrency and capacity:** each chain holds a single job slot, acquired without waiting. A second replay for a chain that is already running is rejected immediately with `429`, never queued. A global cap limits jobs across all chains; its default and its hard maximum are both `20`. There is no cancellation endpoint, no per-job timeout, and no cap on the size of a requested range, so a job holds its chain's slot until it finishes or the process restarts.

**Failure handling:** a replay is refused up front with `503` when NATS is not connected. Once running, a transient publish failure is retried up to `publish_max_retries` times; a batch read that exhausts `max_retries`, or a publish that exhausts its retries, stops the job and records where it stopped. There is no in-memory buffer and no automatic resume. Recovery is a new `POST /replay` for the remaining range.

**Observing progress:** `POST /replay` returns `202 Accepted` before the replay runs, so progress and the terminal outcome are read by polling `GET /replay/status`. Nothing is pushed on completion, there is no job id, and status is held in memory only.

**Graceful shutdown:** `SIGINT` and `SIGTERM` both stop the job cooperatively at its current block. The HTTP listener closes first, then every chain's in-flight job is awaited concurrently for 30 seconds and aborted past that deadline. Nothing is persisted, so the resume block is no longer readable once the listener is down. After a signal-triggered stop, re-post the full original range rather than a resume point. The `RpcError` and `NatsError` stops leave the process running, so for those the resume block is still readable from `GET /replay/status`.

---

## Prerequisites

- Rust >= 1.85 (edition 2024)
- A running NATS server with JetStream enabled
- Access to an Ethereum-compatible RPC endpoint for each chain you configure
- The [nox-protocol-contracts](https://github.com/iExec-Nox/nox-protocol-contracts) NoxCompute deployed on each of those chains

---

## Getting Started

**Local development.** The defaults target a production NATS cluster with mTLS, so a single-node dev server needs TLS disabled, one URL, and one replica. Start a JetStream-enabled server with `nats-server -js` first. This repository ships no compose file.

```bash
git clone https://github.com/iExec-Nox/nox-ingestor-replayer.git
cd nox-ingestor-replayer

# One chain, keyed by its numeric chain id
export NOX_REPLAYER_CHAINS__421614__RPC_ENDPOINT="https://arbitrum-sepolia-rpc.publicnode.com"
export NOX_REPLAYER_CHAINS__421614__CONTRACT_ADDRESS="0x..."
export NOX_REPLAYER_CHAINS__421614__BATCH_SIZE="50"
export NOX_REPLAYER_CHAINS__421614__RETRY_DELAY="250ms"
export NOX_REPLAYER_CHAINS__421614__MAX_RETRIES="5"

# The API key must be at least 18 characters or the process refuses to start
export NOX_REPLAYER_REPLAY__API_KEY="local-dev-api-key-0001"

# Single-node NATS: the defaults assume a 3-node mTLS cluster
export NOX_REPLAYER_NATS__TLS__ENABLED="false"
export NOX_REPLAYER_NATS__URLS="nats://127.0.0.1:4222"
export NOX_REPLAYER_NATS__NUM_REPLICAS="1"

cargo run --release
```

**First replay.** `chain_id` goes in the query string and the block range goes in the body:

```bash
curl -i -X POST 'http://localhost:8080/replay?chain_id=421614' \
  -H 'X-Api-Key: local-dev-api-key-0001' \
  -H 'Content-Type: application/json' \
  -d '{"from_block": 1000, "to_block": 1200}'
```

A successful call returns `202 Accepted`; poll `GET /replay/status?chain_id=421614` to follow the job.

---

## Environment Variables

Configuration is loaded from environment variables with the `NOX_REPLAYER_` prefix. Nested properties use double underscore (`__`) as separator. Chains are configured as a map keyed by numeric chain id under `NOX_REPLAYER_CHAINS__<chain_id>__*`; configure at least one chain. Replace `<chain_id>` below with a real chain id (e.g. `421614` for Arbitrum Sepolia).

Every constraint listed below is enforced before the HTTP listener binds. A violation aborts startup with an `Invalid configuration` error naming the failed rule, so a misconfigured deployment crash-loops rather than running degraded.

| Variable | Description | Required | Default |
| -------- | ----------- | -------- | ------- |
| `NOX_REPLAYER_SERVER__HOST` | HTTP server bind address. Containerized deployments must set `0.0.0.0`; the default only accepts loopback traffic. Restrict reachability at the network layer when you do. `GET /metrics` and `GET /replay/status` are unauthenticated. | No | `127.0.0.1` |
| `NOX_REPLAYER_SERVER__PORT` | HTTP server port | No | `8080` |
| `NOX_REPLAYER_CHAINS__<chain_id>__RPC_ENDPOINT` | Ethereum RPC URL for this chain. Must be a valid URL. | **Yes** | _(none)_ |
| `NOX_REPLAYER_CHAINS__<chain_id>__CONTRACT_ADDRESS` | NoxCompute contract address to read on this chain. Must not be the zero address. | **Yes** | _(none)_ |
| `NOX_REPLAYER_CHAINS__<chain_id>__BATCH_SIZE` | Blocks fetched per RPC call, `1`–`10000`. The practical upper bound is provider-dependent (`eth_getLogs` range and result caps). | **Yes** | _(none)_ |
| `NOX_REPLAYER_CHAINS__<chain_id>__RETRY_DELAY` | Delay between retries on a failed batch read. Must be greater than zero. | **Yes** | _(none)_ |
| `NOX_REPLAYER_CHAINS__<chain_id>__MAX_RETRIES` | Retry attempts for a failing batch read, maximum `10` | **Yes** | _(none)_ |
| `NOX_REPLAYER_CHAINS__<chain_id>__CONNECT_TIMEOUT` | TCP connection timeout for RPC requests. Must be greater than zero and must not exceed `RPC_TIMEOUT`. | No | `5s` |
| `NOX_REPLAYER_CHAINS__<chain_id>__RPC_TIMEOUT` | Total per-request RPC timeout (connect + read). Must be greater than zero. | No | `8s` |
| `NOX_REPLAYER_REPLAY__API_KEY` | API key required in the `X-Api-Key` header to authorize `POST /replay`. Minimum length `18`; a shorter or empty value fails validation and the process refuses to start, so authentication cannot be turned off. | **Yes** | _(none)_ |
| `NOX_REPLAYER_REPLAY__MAX_CONCURRENT_REPLAY_JOBS` | Global cap on concurrently-running replay jobs across all chains, `1`–`20`. The maximum is not configurable beyond `20`. | No | `20` |
| `NOX_REPLAYER_NATS__URLS` | NATS server URLs, comma-separated. One URL = single-node; several = cluster with transparent failover. Every entry must use the `tls://` scheme (immediate TLS) or `nats://` (STARTTLS / plaintext). | No | `nats://localhost:4221,nats://localhost:4222,nats://localhost:4223` |
| `NOX_REPLAYER_NATS__TLS__ENABLED` | Enable mTLS. When `false`, connects in plaintext and the CA/CERT/KEY vars are ignored. | No | `true` |
| `NOX_REPLAYER_NATS__TLS__CA` | CA certificate **PEM content** (not a path). Required when TLS enabled. | When TLS on | _(empty)_ |
| `NOX_REPLAYER_NATS__TLS__CERT` | Client certificate PEM content (`CN=nox-ingestor-replayer`). Required when TLS enabled. | When TLS on | _(empty)_ |
| `NOX_REPLAYER_NATS__TLS__KEY` | Client private key PEM content. Required when TLS enabled. | When TLS on | _(empty)_ |
| `NOX_REPLAYER_NATS__NUM_REPLICAS` | JetStream stream replica count used when creating the stream, `1`–`5`. `1` for single-node, `3` for a 3-node cluster. If the stream already exists with a different count, the replayer logs a warning and continues (it never mutates an existing stream). | No | `3` |
| `NOX_REPLAYER_NATS__STREAM_NAME` | JetStream stream name, at least 1 character | No | `nox_ingestor` |
| `NOX_REPLAYER_NATS__SUBJECT` | Subject prefix for published messages, at least 1 character | No | `nox_ingestor` |
| `NOX_REPLAYER_NATS__RETENTION` | Stream message retention window. Messages age out after this window regardless of whether any consumer acknowledged them. Must be greater than zero and must not be shorter than `DUPLICATE_WINDOW`. | No | `1d` |
| `NOX_REPLAYER_NATS__DUPLICATE_WINDOW` | JetStream deduplication window, matched on the `Nats-Msg-Id` header (see [`GET /replay/status`](#get-replaystatus)). Must be greater than zero and must not exceed `RETENTION`. | No | `10m` |
| `NOX_REPLAYER_NATS__RECONNECT_DELAY` | Initial delay before reconnecting to NATS. Must be greater than zero and must not exceed `MAX_RECONNECT_DELAY`. | No | `1s` |
| `NOX_REPLAYER_NATS__MAX_RECONNECT_DELAY` | Maximum reconnect backoff. Must be greater than zero. | No | `30s` |
| `NOX_REPLAYER_NATS__PUBLISH_RETRY_DELAY` | Delay between retries on a transient publish failure. Must be greater than zero. | No | `250ms` |
| `NOX_REPLAYER_NATS__PUBLISH_MAX_RETRIES` | Max retry attempts for a transient publish failure before giving up, maximum `10` | No | `5` |

An entire configuration file can be supplied instead of individual variables by pointing `NOX_REPLAYER_FILE` at it, with the format inferred from the file extension:

```bash
NOX_REPLAYER_FILE=/run/secrets/replayer.toml
```

`NOX_REPLAYER_FILE` is whole-file only: there is no working per-key `<VAR>_FILE` indirection, so do not expect `NOX_REPLAYER_REPLAY__API_KEY_FILE` to load a secret from a path. Values from the file take precedence over the individual environment variables above.

Logging level is controlled via the `RUST_LOG` environment variable:

```bash
RUST_LOG=info    # Default
RUST_LOG=debug   # Verbose logging
```

Every published event emits one `INFO` line, so a wide replay produces one log line per event rather than per transaction or per batch. Size log ingestion accordingly before replaying a large range.

---

## HTTP Endpoints

The service exposes three unauthenticated monitoring endpoints and two replay endpoints. Any other path returns `404` with a JSON error body. A known path called with the wrong method (`GET /replay` for example) returns a bodyless `405` with an `Allow` header instead, so use the method given in each table below.

### Service Endpoints

#### `GET /`

Returns basic service information.

**Response:**

```json
{
  "service": "Ingestor Replayer",
  "timestamp": "2026-02-25T10:30:00.123456789+00:00"
}
```

#### `GET /health`

Health check endpoint for monitoring and orchestration.

**Response:**

```json
{
  "status": "ok"
}
```

This is a liveness signal only. It returns `ok` unconditionally and reflects nothing about NATS or RPC connectivity.

#### `GET /metrics`

Prometheus metrics endpoint for observability.

**Response:** Prometheus text format metrics.

In addition to the per-route HTTP request metrics emitted automatically by the middleware layer (`axum_http_requests_*`), the service exports the following application metrics. Names match the Prometheus text format exactly, in source and here. All per-chain counters and gauges are pre-registered at startup, so each series is present (at `0`) before its first event, except `nox_replayer_replay_last_published_block`, which stays absent until a chain's first successful publish (block `0` would falsely assert that block was published; query it with `absent()`/`OR on()` in dashboards). Three further series are also created lazily: the two histograms, and `nox_replayer_replay_requests_total`, which is not per-chain and has no `outcome` series at all until the first request arrives.

##### NATS

| Metric | Type | Description |
| ------ | ---- | ----------- |
| `nox_replayer_nats_connection_state` | gauge | Current connection state: `1` connected, `0` disconnected. |
| `nox_replayer_nats_reconnects_total` | counter | Reconnections after the initial connect (the first connect is not counted). |
| `nox_replayer_nats_publish_retries_total` | counter | Transient publish failures that were retried. |

##### Replay

> [!NOTE]
> All metrics below are labeled by `chain_id`, except `nox_replayer_replay_requests_total`.

| Metric | Type | Description |
| ------ | ---- | ----------- |
| `nox_replayer_replay_requests_total` | counter | `POST /replay` responses by `outcome` label: `ok` on accept, otherwise the error kind (`unauthorized`, `missing_chain_id`, `invalid_chain_id`, `invalid_range`, `range_beyond_head`, `chain_busy`, `chain_not_configured`, `at_capacity`, `rpc_error`, `nats_error`). |
| `nox_replayer_replay_jobs_in_flight` | gauge | Replay jobs currently running. |
| `nox_replayer_replay_jobs_duration_seconds` | histogram | Wall-clock duration of a replay job, labeled by `chain_id` and terminal `result` (`completed`, `shutdown`, `rpc_error`, `nats_error`). These are a separate vocabulary from the `stop_reason` values returned by [`GET /replay/status`](#get-replaystatus), in particular `shutdown` corresponds to `"ShutdownSignal"`, not to a case transform of it. |
| `nox_replayer_replay_blocks_read_total` | counter | Blocks read from RPC. |
| `nox_replayer_replay_rpc_errors_total` | counter | Batch reads that failed terminally and aborted their job. Retries are logged, not counted, so each increment is one aborted replay and one block range still missing from the stream. |
| `nox_replayer_replay_rpc_reads_seconds` | histogram | Latency of each bounded batch read from RPC. |
| `nox_replayer_replay_publish_requests_total` | counter | Publish attempts to NATS by `outcome` label: `success` (newly published), `duplicate` (JetStream deduplicated, already stored), `failure` (failed after exhausting retries). |
| `nox_replayer_replay_events_total` | counter | Events published to NATS. |
| `nox_replayer_replay_last_published_block` | gauge | Block number of the most recent successful publish. Absent until that chain's first successful publish. |

##### Build

| Metric | Type | Description |
| ------ | ---- | ----------- |
| `nox_replayer_build_info` | gauge | Always `1`, labeled by `version` (the crate version) for dashboard/version pinning. |

### Replay Endpoints

Replaying a block range is what this service is for. A range is submitted with an authenticated `POST /replay`, runs as a background task, and is followed by polling `GET /replay/status`.

| Method | Path | Auth | Purpose |
| ------ | ---- | ---- | ------- |
| `POST` | `/replay?chain_id=<id>` | `X-Api-Key` | Accept an inclusive block range for replay |
| `GET` | `/replay/status[?chain_id=<id>]` | none | Read the current or most recent job status |

#### `POST /replay`

Accepts an inclusive block range for one configured chain, validates it, and returns `202 Accepted` **before the replay runs**. Every NoxCompute transaction in the range is re-read from that chain's RPC endpoint and republished to the same JetStream subject the live ingestor uses.

The API key goes in the `X-Api-Key` header; the header name is matched case-insensitively and the value is compared in constant time. `chain_id` is a **query parameter**, not a body field, and must be a decimal `u32` matching a configured chain. The body is JSON with **snake_case** field names, and `Content-Type: application/json` is required. Note the casing split: the query parameter and both request and response bodies are snake_case, while the NATS messages this service publishes are camelCase.

```bash
curl -i -X POST 'http://localhost:8080/replay?chain_id=421614' \
  -H 'X-Api-Key: <your-api-key>' \
  -H 'Content-Type: application/json' \
  -d '{"from_block": 1000, "to_block": 1200}'
```

| Field | Description |
| ----- | ----------- |
| `chain_id` (query) | Configured chain to replay. Required; absent or non-numeric values are rejected. |
| `from_block` (body) | First block to replay, inclusive |
| `to_block` (body) | Last block to replay, inclusive. Must be at least `from_block` and must not exceed the chain's current head. |

There is no maximum range size. A wide range is simply a long-running job. The head bound is a validation rule, not a finality buffer: there is no confirmation depth anywhere in the service, so replaying right up to the head can publish events from blocks that a reorg later drops.

**Success Response (`202 Accepted`):**

```json
{
  "request": {
    "chain_id": 421614,
    "from_block": 1000,
    "to_block": 1200
  },
  "accepted_at": "2026-08-04T09:10:00.123456789Z"
}
```

`202` means the range passed every pre-flight check and a background job has started. It says nothing about whether the replay will succeed. Poll `GET /replay/status?chain_id=421614` until `state` is no longer `"Running"`.

**Error Responses:**

Every replay error shares one envelope. There is no machine-readable `kind` or `code` field in it, so callers must switch on the HTTP status rather than on `message`. The `outcome` values below name the `nox_replayer_replay_requests_total` label, which is observable on `/metrics` but not in the response.

```json
{
  "error": {
    "message": "Chain 421614 busy",
    "retryable": true
  }
}
```

| Condition | Status | Retryable | Outcome label |
| --------- | ------ | --------- | ------------- |
| `X-Api-Key` missing, empty, or wrong | `401` | no | `unauthorized` |
| `chain_id` query parameter absent | `400` | no | `missing_chain_id` |
| `chain_id` present but not a valid `u32` | `400` | no | `invalid_chain_id` |
| `from_block` greater than `to_block` | `400` | no | `invalid_range` |
| No chain configured for `chain_id` | `400` | no | `chain_not_configured` |
| `to_block` beyond the chain's current head (the message reports both) | `400` | no | `range_beyond_head` |
| A replay is already running for this chain | `429` | yes | `chain_busy` |
| Global concurrent-job cap reached (the message reports the cap) | `429` | yes | `at_capacity` |
| Fetching the chain head from RPC failed | `502` | yes | `rpc_error` |
| NATS is not connected | `503` | yes | `nats_error` |

Pre-flight checks run in this order, which decides which error you get when more than one applies: API key, then `chain_id`, then the range ordering, then the chain lookup, then the per-chain job slot, then the global cap, then NATS connectivity, then the chain head. Two consequences are worth noting: `invalid_range` is reported before `chain_not_configured`, and `chain_busy` is reported before the NATS and head checks. So a busy chain masks an unreachable RPC endpoint. No pre-flight failure has any side effect: nothing is published.

A malformed body, or a request without `Content-Type: application/json`, is rejected by the HTTP layer **before** the API key is checked, with one of three statuses: `400` when the body is not syntactically valid JSON, `415` when `Content-Type` is missing or not `application/json`, and `422` when the body is valid JSON that does not match the schema. All three carry a `text/plain` body rather than the envelope above, so a `400` from this path is **not** enveloped even though every `400` in the table is. Parse the body only after confirming `Content-Type: application/json` on the response. None of the three is counted in `nox_replayer_replay_requests_total`, so they say nothing about whether your key is valid.

The `404` fallback for unknown routes uses a different shape, which does carry a `kind`:

```json
{
  "error": {
    "kind": "not_found",
    "message": "Route not found /nope",
    "retryable": false
  }
}
```

#### `GET /replay/status`

Unauthenticated. Returns a JSON object **keyed by chain id**, so a single-chain query is still wrapped in a map. Omit `chain_id` to get every configured chain; an unknown `chain_id` returns `400`.

```bash
curl -s 'http://localhost:8080/replay/status?chain_id=421614'
```

```json
{
  "421614": {
    "state": "Running",
    "from_block": 1000,
    "to_block": 1200,
    "current_block": 1050,
    "transactions_published": 12,
    "events_total": 31,
    "duplicates": 0,
    "duration_ms": 842,
    "started_at": "2026-08-04T09:10:00.123456789Z",
    "finished_at": null,
    "stop_reason": null
  }
}
```

| Field | Description |
| ----- | ----------- |
| `state` | `"Idle"`, `"Running"`, `"Completed"` or `"Stopped"`. PascalCase. |
| `from_block` / `to_block` | Range of the current or most recent job, `0` while `"Idle"` |
| `current_block` | While `"Running"`, the next unprocessed block. On `"Completed"`, `to_block + 1`. On `"Stopped"`, the block to resume from. |
| `transactions_published` | Transactions published so far, including those JetStream deduplicated |
| `events_total` | Events across those transactions |
| `duplicates` | Publishes JetStream recognized as already stored |
| `duration_ms` | Elapsed wall-clock time of the job |
| `started_at` / `finished_at` | RFC 3339 timestamps. `finished_at` is `null` until the job reaches a terminal state. |
| `stop_reason` | `null` while `"Idle"` or `"Running"`; otherwise `"Completed"`, `"ShutdownSignal"`, `"RpcError"` or `"NatsError"`. PascalCase. |

Progress is written once per batch, not per transaction, so with `BATCH_SIZE=50` the `current_block` field advances in steps of 50 (the last step is shorter when the range does not divide evenly). `"Stopped"` is a resume point rather than an exception: re-post `{"from_block": <current_block>, "to_block": <to_block>}` to continue. That resume point is readable only while the process is alive. After a `"ShutdownSignal"` stop the process is gone and the status with it, so re-post the full original range instead.

Re-posting an overlapping range relies on JetStream deduplication, which is **time-bounded and not a correctness guarantee**. Each message carries `Nats-Msg-Id: keccak256("{chainId}:{transactionHash}")`, so JetStream drops a re-publish only while that id is still inside `DUPLICATE_WINDOW` (default `10m`). Past the window a second full copy of the transaction lands on the stream under a new sequence number. Since a recovery replay is usually issued more than ten minutes after the failure it recovers from, **duplicate delivery is the expected case, not the exception**. Consumers must be idempotent on `(chainId, transactionHash)`.

There is no job id and no history. A terminal status is kept only until the next replay is accepted for that chain, and it is lost entirely if the process restarts. Completion is observed by polling; there is no webhook and no completion message.

---

## NATS Message Format

The replayer publishes one JSON message per transaction to the configured JetStream stream. Each message groups all NoxCompute events emitted by a single transaction, preserving their original log order.

### Subject

```text
{nats.subject}.{transaction_hash}
```

With default configuration: `nox_ingestor.0x<tx_hash>`.

### Payload

```json
{
  "chainId": 421614,
  "caller": "0x...",
  "blockNumber": 12345678,
  "transactionHash": "0x...",
  "events": [
    {
      "logIndex": 0,
      "caller": "0x...",
      "type": "add",
      "leftHandOperand": "0x...",
      "rightHandOperand": "0x...",
      "result": "0x..."
    }
  ]
}
```

| Field | Description |
| ----- | ----------- |
| `chainId` | Chain ID where the events occurred |
| `caller` | Ethereum address that submitted the transaction, as lowercase `0x` hex. Not EIP-55 checksummed |
| `blockNumber` | Block containing the transaction |
| `transactionHash` | Transaction hash, lowercase `0x` hex (also the NATS subject suffix) |
| `events` | Ordered list of NoxCompute events from this transaction |
| `events[].logIndex` | Original log index within the block |
| `events[].caller` | Address passed as the `caller` indexed parameter in the event log, same lowercase form |
| `events[].type` | Event type (snake_case, see table below) |

### Event Types

Every additional field listed below is an encrypted value handle: a `bytes32` rendered as a lowercase `0x` hex **string**. That includes `success`, `condition`, `ifTrue`, `ifFalse`, `totalSupply` and the `newBalance*` fields. Despite its name, `success` is a handle to an encrypted boolean, not a JSON `true`/`false`. `teeType` is the one exception: it is a JSON number, not a hex string.

| `type` | Operation | Additional fields |
| ------ | --------- | ----------------- |
| `wrap_as_public_handle` | Wrap a plaintext as a publicly readable handle | `value`, `teeType`, `handle` |
| `add` | Encrypted addition | `leftHandOperand`, `rightHandOperand`, `result` |
| `sub` | Encrypted subtraction | `leftHandOperand`, `rightHandOperand`, `result` |
| `mul` | Encrypted multiplication | `leftHandOperand`, `rightHandOperand`, `result` |
| `div` | Encrypted division | `leftHandOperand`, `rightHandOperand`, `result` |
| `safe_add` | Overflow-checked addition | `leftHandOperand`, `rightHandOperand`, `success`, `result` |
| `safe_sub` | Overflow-checked subtraction | `leftHandOperand`, `rightHandOperand`, `success`, `result` |
| `safe_mul` | Overflow-checked multiplication | `leftHandOperand`, `rightHandOperand`, `success`, `result` |
| `safe_div` | Overflow-checked division | `leftHandOperand`, `rightHandOperand`, `success`, `result` |
| `eq` | Encrypted equality comparison | `leftHandOperand`, `rightHandOperand`, `result` |
| `ne` | Encrypted inequality comparison | `leftHandOperand`, `rightHandOperand`, `result` |
| `ge` | Encrypted greater-or-equal comparison | `leftHandOperand`, `rightHandOperand`, `result` |
| `gt` | Encrypted greater-than comparison | `leftHandOperand`, `rightHandOperand`, `result` |
| `le` | Encrypted less-or-equal comparison | `leftHandOperand`, `rightHandOperand`, `result` |
| `lt` | Encrypted less-than comparison | `leftHandOperand`, `rightHandOperand`, `result` |
| `select` | Conditional select | `condition`, `ifTrue`, `ifFalse`, `result` |
| `transfer` | Confidential token transfer | `balanceFrom`, `balanceTo`, `amount`, `success`, `newBalanceFrom`, `newBalanceTo` |
| `mint` | Confidential token mint | `balanceTo`, `amount`, `totalSupply`, `success`, `newBalanceTo`, `newTotalSupply` |
| `burn` | Confidential token burn | `balanceFrom`, `amount`, `totalSupply`, `success`, `newBalanceFrom`, `newTotalSupply` |

---

## Related Repositories

| Repository | Role |
| ---------- | ---- |
| [nox-protocol-contracts](https://github.com/iExec-Nox/nox-protocol-contracts) | Protocol contracts. NoxCompute is the on-chain source of the events this service replays |
| [nox-ingestor](https://github.com/iExec-Nox/nox-ingestor) | Continuous block scanner publishing to the same stream as blocks arrive; this service is its on-demand complement for historical ranges |
| [nox-runner](https://github.com/iExec-Nox/nox-runner) | Off-chain computation runner. Consumes the stream to drive confidential computations |

---

## License

The Nox Protocol source code is released under the Business Source License 1.1 (BUSL-1.1).

The license will automatically convert to the MIT License under the conditions described in the [LICENSE](./LICENSE) file.

The full text of the MIT License is provided in the [LICENSE-MIT](./LICENSE-MIT) file.
