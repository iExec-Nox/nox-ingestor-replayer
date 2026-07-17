# Nox · Ingestor Replayer

[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](./LICENSE) [![Docs](https://img.shields.io/badge/docs-nox--protocol-purple)](https://docs.iex.ec) [![Discord](https://img.shields.io/badge/chat-Discord-5865F2)](https://discord.com/invite/5TewNUnJHN) [![Ship](https://img.shields.io/github/v/tag/iExec-Nox/nox-ingestor-replayer?label=ship)](https://github.com/iExec-Nox/nox-ingestor-replayer/releases)

> Blockchain event listener that streams NoxCompute operations to NATS JetStream.

## Table of Contents

- [Nox · Ingestor Replayer](#nox--ingestor-replayer)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Prerequisites](#prerequisites)
  - [Getting Started](#getting-started)
  - [Environment Variables](#environment-variables)
  - [HTTP Endpoints](#http-endpoints)
    - [`GET /`](#get-)
    - [`GET /health`](#get-health)
    - [`GET /metrics`](#get-metrics)
  - [NATS Message Format](#nats-message-format)
    - [Subject](#subject)
    - [Payload](#payload)
    - [Event Types](#event-types)
  - [Related Repositories](#related-repositories)
  - [License](#license)

---

## Overview

The Replayer is the on-chain observation layer of the Nox Protocol. It polls an Arbitrum RPC node in batches, parses every NoxCompute event log, groups events by transaction, and publishes each group as a single JSON message to a NATS JetStream stream. Downstream consumers (runners, orchestrators, exporters) subscribe to that stream without any direct dependency on the chain.

**Block scanning (`chain → NATS`):** The replayer maintains a persistent cursor (last processed block) in a local state file. On start it resumes from that cursor, or from a configured initial block. Blocks are fetched in configurable batches; each batch is parsed and all resulting messages are published before the cursor advances. On clean shutdown the cursor is flushed to disk.

**NATS resilience:** When NATS is unavailable the replayer buffers messages in memory (up to a configurable capacity), pauses block scanning to avoid unbounded growth, and resumes automatically once the connection is restored. JetStream deduplication ensures that a restart or a duplicate batch never produces duplicate messages on the stream.

**State persistence:** The cursor is written atomically (write to `.tmp` → fsync → rename → directory sync) to prevent corruption on crash. If no state file exists and no initial block is configured, the replayer refuses to start rather than silently scanning from block zero.

**Graceful shutdown:** SIGINT and SIGTERM both trigger a coordinated shutdown: scanning stops, in-flight messages are flushed, and the cursor is persisted with a five-second timeout before the process exits.

---

## Prerequisites

- Rust >= 1.85 (edition 2024)
- A running NATS server with JetStream enabled
- Access to an Ethereum-compatible RPC endpoint (Arbitrum Sepolia or mainnet)
- The [nox-protocol-contracts](https://github.com/iExec-Nox/nox-protocol-contracts) NoxCompute deployed on that chain

---

## Getting Started

```bash
git clone https://github.com/iExec-Nox/nox-ingestor-replayer.git
cd nox-ingestor-replayer

# Configure one or more chains (keyed by numeric chain id) and the replay API key
export NOX_REPLAYER_CHAINS__421614__RPC_ENDPOINT="https://arbitrum-sepolia-rpc.publicnode.com"
export NOX_REPLAYER_CHAINS__421614__CONTRACT_ADDRESS="0x..."
export NOX_REPLAYER_CHAINS__421614__BATCH_SIZE="50"
export NOX_REPLAYER_CHAINS__421614__RETRY_DELAY="250ms"
export NOX_REPLAYER_CHAINS__421614__MAX_RETRIES="5"
export NOX_REPLAYER_REPLAY__API_KEY="<your-secret-key>"

# Build and run
cargo run --release
```

---

## Environment Variables

Configuration is loaded from environment variables with the `NOX_REPLAYER_` prefix. Nested properties use double underscore (`__`) as separator. Chains are configured as a map keyed by numeric chain id under `NOX_REPLAYER_CHAINS__<chain_id>__*`; configure at least one chain. Replace `<chain_id>` below with a real chain id (e.g. `421614` for Arbitrum Sepolia).

| Variable | Description | Required | Default |
| -------- | ----------- | -------- | ------- |
| `NOX_REPLAYER_SERVER__HOST` | HTTP server bind address | No | `127.0.0.1` |
| `NOX_REPLAYER_SERVER__PORT` | HTTP server port | No | `8080` |
| `NOX_REPLAYER_CHAINS__<chain_id>__RPC_ENDPOINT` | Ethereum RPC URL for this chain | **Yes** | _(none)_ |
| `NOX_REPLAYER_CHAINS__<chain_id>__CONTRACT_ADDRESS` | NoxCompute contract address to monitor on this chain | **Yes** | _(none)_ |
| `NOX_REPLAYER_CHAINS__<chain_id>__BATCH_SIZE` | Blocks fetched per RPC call | **Yes** | _(none)_ |
| `NOX_REPLAYER_CHAINS__<chain_id>__RETRY_DELAY` | Delay between retries on a failed batch read | **Yes** | _(none)_ |
| `NOX_REPLAYER_CHAINS__<chain_id>__MAX_RETRIES` | Retry attempts for a failing batch read | **Yes** | _(none)_ |
| `NOX_REPLAYER_CHAINS__<chain_id>__CONNECT_TIMEOUT` | TCP connection timeout for RPC requests | No | `5s` |
| `NOX_REPLAYER_CHAINS__<chain_id>__RPC_TIMEOUT` | Total per-request RPC timeout (connect + read) | No | `8s` |
| `NOX_REPLAYER_REPLAY__API_KEY` | API key required in the `X-Api-Key` header to authorize `POST /replay`. An empty value always rejects the request (no way to disable auth). | **Yes** | _(none)_ |
| `NOX_REPLAYER_REPLAY__MAX_CONCURRENT_REPLAY_JOBS` | Global cap on concurrently-running replay jobs across all chains | No | `20` |
| `NOX_REPLAYER_NATS__URLS` | NATS server URLs, comma-separated. One URL = single-node; several = cluster with transparent failover. Use the `tls://` scheme for immediate-TLS servers, `nats://` for STARTTLS / plaintext. | No | `nats://localhost:4221,nats://localhost:4222,nats://localhost:4223` |
| `NOX_REPLAYER_NATS__TLS__ENABLED` | Enable mTLS. When `false`, connects in plaintext and the CA/CERT/KEY vars are ignored. | No | `true` |
| `NOX_REPLAYER_NATS__TLS__CA` | CA certificate **PEM content** (not a path). Required when TLS enabled. | When TLS on | _(empty)_ |
| `NOX_REPLAYER_NATS__TLS__CERT` | Client certificate PEM content (`CN=nox-ingestor-replayer`). Required when TLS enabled. | When TLS on | _(empty)_ |
| `NOX_REPLAYER_NATS__TLS__KEY` | Client private key PEM content. Required when TLS enabled. | When TLS on | _(empty)_ |
| `NOX_REPLAYER_NATS__NUM_REPLICAS` | JetStream stream replica count used when creating the stream. `1` for single-node, `3` for a 3-node cluster. If the stream already exists with a different count, the replayer logs a warning and continues (it never mutates an existing stream). | No | `3` |
| `NOX_REPLAYER_NATS__STREAM_NAME` | JetStream stream name | No | `nox_ingestor` |
| `NOX_REPLAYER_NATS__SUBJECT` | Subject prefix for published messages | No | `nox_ingestor` |
| `NOX_REPLAYER_NATS__RETENTION` | Stream message retention window | No | `1d` |
| `NOX_REPLAYER_NATS__DUPLICATE_WINDOW` | JetStream deduplication window | No | `10m` |
| `NOX_REPLAYER_NATS__RECONNECT_DELAY` | Initial delay before reconnecting to NATS | No | `1s` |
| `NOX_REPLAYER_NATS__MAX_RECONNECT_DELAY` | Maximum reconnect backoff | No | `30s` |
| `NOX_REPLAYER_NATS__PUBLISH_RETRY_DELAY` | Delay between retries on a transient publish failure | No | `250ms` |
| `NOX_REPLAYER_NATS__PUBLISH_MAX_RETRIES` | Max retry attempts for a transient publish failure before giving up | No | `5` |

For sensitive values, you can use the `_FILE` suffix to load from a file:

```bash
NOX_REPLAYER_CHAIN__RPC_ENDPOINT_FILE=/run/secrets/rpc_endpoint
```

Logging level is controlled via the `RUST_LOG` environment variable:

```bash
RUST_LOG=info    # Default
RUST_LOG=debug   # Verbose logging
```

---

## HTTP Endpoints

The ingestor exposes a minimal HTTP server for monitoring. It does not expose any data ingestion or query API.

### `GET /`

Returns basic service information.

**Response:**

```json
{
  "service": "Ingestor",
  "timestamp": "2026-02-25T10:30:00.000Z"
}
```

### `GET /health`

Health check endpoint for monitoring and orchestration.

**Response:**

```json
{
  "status": "ok"
}
```

### `GET /metrics`

Prometheus metrics endpoint for observability.

**Response:** Prometheus text format metrics.

---

## NATS Message Format

The ingestor publishes one JSON message per transaction to the configured JetStream stream. Each message groups all NoxCompute events emitted by a single transaction, preserving their original log order.

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
| `caller` | Ethereum address that submitted the transaction |
| `blockNumber` | Block containing the transaction |
| `transactionHash` | Transaction hash (also the NATS subject suffix) |
| `events` | Ordered list of NoxCompute events from this transaction |
| `events[].logIndex` | Original log index within the block |
| `events[].caller` | Address passed as the `caller` indexed parameter in the event log |
| `events[].type` | Event type (snake_case, see table below) |

### Event Types

All handle values (`leftHandOperand`, `rightHandOperand`, `result`, etc.) are `bytes32` hex strings representing encrypted value handles.

| `type` | Operation | Additional fields |
| ------ | --------- | ----------------- |
| `plaintext_to_encrypted` | Encrypt a plaintext value into a handle | `value`, `teeType`, `handle` |
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
| [nox-protocol-contracts](https://github.com/iExec-Nox/nox-protocol-contracts) | Protocol contracts — NoxCompute is the on-chain source of events the replayer indexes |
| [nox-runner](https://github.com/iExec-Nox/nox-runner) | Off-chain computation runner — consumes replayer events to drive confidential computations |

---

## License

The Nox Protocol source code is released under the Business Source License 1.1 (BUSL-1.1).

The license will automatically convert to the MIT License under the conditions described in the [LICENSE](./LICENSE) file.

The full text of the MIT License is provided in the [LICENSE-MIT](./LICENSE-MIT) file.
