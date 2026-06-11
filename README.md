# Simple Queue Service

A small FIFO queue service built with FastAPI and SQLite.

## Features

- Create and delete topics.
- Clear topic messages.
- Enqueue messages.
- Acquire messages in FIFO order.
- Ack acquired messages.
- Persist retrieval count.
- Move messages to a per-topic dead-letter topic after 10 acquires without ack.

## Storage

- SQLite database: `./.data/queue.db`
- The `.data` directory is created automatically at startup.

## API

- `GET /api/v1/topics`
- `POST /api/v1/topics`
- `DELETE /api/v1/topics/{topic_id}`
- `POST /api/v1/topics/{topic_id}/clear`
- `POST /api/v1/topics/{topic_id}/message`
- `GET /api/v1/topics/{topic_id}/message`
- `POST /api/v1/topics/{topic_id}/message/{message_id}/ack`

## Run

```bash
make run
```

## Docker

```bash
docker build -t simple-queue-service .
docker run -p 8000:8000 -v $(pwd)/.data:/app/.data simple-queue-service
```

## Docker Compose

```bash
docker compose up --build
docker compose port app 8000
```

## Install

```bash
uv sync --extra test
```

## Test

```bash
make test
```

## Package Manager

This project uses `uv` as the default package manager.
