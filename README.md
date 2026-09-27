# Distributed Idempotent Task Queue

An event-driven transaction pipeline built in Go designed to handle concurrent network retries and duplicate requests without double-processing. 

Uses Redis Streams for durable queuing, Redis Lua scripts for distributed locking, and PostgreSQL with atomic transactions for storage and idempotency guarantees.

---

## The Problem

In payment pipelines and async task processing, network timeouts and retries often cause identical requests to hit the backend simultaneously. Without idempotency and synchronization, this leads to race conditions, double charges, and corrupted state.

This project solves that using a defense-in-depth approach:
1. **Redis Stream Consumer Groups** ensure at-least-once message delivery.
2. **Distributed Redis Locks (Lua)** prevent parallel execution of the same idempotency key across multiple workers.
3. **PostgreSQL Transactions + Unique Constraints** guarantee that even if a lock expires or an edge case occurs, duplicate rows can never be committed.

---

## Architecture

- **`cmd/api`**: HTTP ingress server. Validates `X-Idempotency-Key` headers and pushes events directly to the Redis stream using `XADD`.
- **`cmd/worker`**: Background consumer using `XREADGROUP`. Grabs the distributed lock, runs a DB check inside a transaction, commits the row, and sends an `XACK`.
- **`internal/lock`**: Redis-backed distributed locking using `SET NX PX` and an atomic Lua release script to ensure workers only release their own locks.
- **Postgres**: Stores processed transactions with a unique index on `idempotency_key`.

---

## Tech Stack

- **Go**: Core application logic and concurrency
- **Redis (Streams + Lua)**: Message broker and distributed locking
- **PostgreSQL**: Relational database with strict uniqueness constraints
- **Docker Compose**: Containerized Redis and Postgres instances
- **k6**: Concurrency and stress testing

---

## Getting Started

### 1. Spin up Postgres & Redis
```bash
docker compose up -d
