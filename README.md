# remote-commands-c_car_platform

Remote Commands service (Phase 6 MVP): issue **remote door unlock** commands end-to-end and track delivery lifecycle.

This MVP is intentionally minimal and designed to be **non-blocking** in previews:
- Service starts even if Kafka is unavailable.
- If Kafka is configured but down, the service logs a warning and falls back to HTTP gateway dispatch.

## What this service does (MVP)

### Endpoints
- `POST /commands/unlock` — request a door unlock command for a `vehicleId`
- `GET /commands/:id` — fetch command status
- `GET /commands?vehicleId=...` — list recent commands (optionally filter by vehicle)
- `POST /acks` — ingest command acknowledgements (HTTP fallback path)

### Command lifecycle
Commands are tracked transiently:
- `PENDING` → created and queued for dispatch
- `SENT` → ack `accepted`
- `ACKED` → ack `completed`
- `FAILED` → ack `failed`/`rejected` OR dispatch error OR ack timeout

## Local development

### Environment
Copy and edit:
- `.env.example` -> `.env`

Key flags:
- `RC_USE_KAFKA=true|false` (default false)
- `RC_GATEWAY_HTTP_URL=http://localhost:3004` (used when Kafka is disabled/unavailable)
- `RC_STORE=memory|file` and `RC_STORE_FILE_PATH=...`
- `RC_ACK_TIMEOUT_MS=15000`

### Run
```bash
npm install
npm run dev
```

## API examples (curl)

### 1) Request unlock (dev mode; no auth required by default)
```bash
curl -s -X POST http://localhost:3020/commands/unlock \
  -H 'content-type: application/json' \
  -d '{"vehicleId":"VIN123"}' | jq .
```

Response (202):
```json
{
  "ok": true,
  "command": {
    "id": "...",
    "correlationId": "...",
    "vehicleId": "VIN123",
    "commandType": "unlock",
    "state": "PENDING",
    "requestedAt": "2026-01-21T00:00:00.000Z"
  }
}
```

### 2) Poll status by id
```bash
curl -s http://localhost:3020/commands/<COMMAND_ID> | jq .
```

### 3) List recent commands for a vehicle
```bash
curl -s "http://localhost:3020/commands?vehicleId=VIN123&limit=20" | jq .
```

## End-to-end dev flow (HTTP fallback)

1) Start `vehicle-gateway` (it exposes `POST /v1/dev/commands` in this Phase 6 update)
2) Start `remote-commands` with:
   - `RC_USE_KAFKA=false`
   - `RC_GATEWAY_HTTP_URL=http://localhost:3004`
3) Call `POST /commands/unlock`
4) Gateway returns an immediate `accepted` ack and later sends a `completed` ack callback to `remote-commands` at `POST /acks` (configurable by gateway env).

## Kafka flow (optional)
If you enable Kafka:
- `remote-commands` produces `RemoteCommandV1` to topic `remote-command.v1`
- `vehicle-gateway` consumes `remote-command.v1` and produces `CommandAckV1` to topic `command-ack.v1`
- `remote-commands` consumes `command-ack.v1` and reconciles status

All payloads are validated using schemas from `@connected-car/shared`.
