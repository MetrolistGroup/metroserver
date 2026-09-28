# Metroserver

[![codecov](https://codecov.io/gh/MetrolistGroup/metroserver/graph/badge.svg)](https://codecov.io/gh/MetrolistGroup/metroserver)

A high performance Go WebSocket server for Metrolist's "Listen Together" feature.  
Utilizes protobuf and gzip compression for fast and efficient communication between clients.

# Quickstart

## Locally
You need to install go, protobuf, and protoc-gen-go

```bash
git clone https://github.com/MetrolistGroup/metroserver
cd metroserver

# Generate protobuf files (required first time)
./scripts/generate_proto.sh

# Download dependencies
go mod download

# Build the server
go build -o metroserver ./cmd/metroserver

# Run on default port 8080
./metroserver

# Run on custom port
PORT=9000 ./metroserver

# Run with a User-Agent policy and the protected User-Agent report
cp ua_policy.example.json ua_policy.json
UA_POLICY_FILE=./ua_policy.json UA_ADMIN_TOKEN='replace-me' ./metroserver

# Read persistent connection counts
curl -H 'Authorization: Bearer replace-me' http://localhost:8080/uas
```

## Configuration

`PORT` sets the HTTP/WebSocket port. It defaults to `8080`.

`UA_POLICY_FILE` points at a JSON User-Agent policy. Without it the server runs
with built-in defaults: Metrolist, N-Zik (`com.nevar.nzik` and
`com.nevar.nzik.debug`), and recognized transport User-Agents can host;
unmatched clients are served a Metrolist ad as the queue title. See `ua_policy.example.json`.

`DATABASE_FILE` sets the shared bbolt database path and defaults to
`metroserver.db`. It stores restart recovery state and connection counts for up
to 10,000 distinct, sanitized User-Agent values. `UA_ADMIN_TOKEN` enables
`/uas`; send it as a bearer token to read the counts. Without the token, `/uas`
is not exposed. The database is created with `0600` permissions.

## Project Structure

- `cmd/metroserver` contains the executable entrypoint.
- `internal/server` contains server behavior and tests.
- `metroproto` contains the protobuf schema submodule.
- `proto` contains generated Go protobuf code.
- `scripts` contains development and generation scripts.

## Docker

```bash
# Clone the repository
git clone https://github.com/MetrolistGroup/metroserver
cd metroserver

# Build locally
docker build -t MetrolistGroup:latest .

# Run on port 8080
docker run -d \
  -p 8080:8080 \
  -e PORT=8080 \
  --name metroserver \
  metroserver:latest

# Run on custom port with a User-Agent policy
docker run -d \
  -p 9000:9000 \
  -e PORT=9000 \
  -e UA_POLICY_FILE=/config/ua_policy.json \
  -e DATABASE_FILE=/app/data/metroserver.db \
  -e UA_ADMIN_TOKEN="$UA_ADMIN_TOKEN" \
  -v "$PWD/ua_policy.example.json:/config/ua_policy.json:ro" \
  -v metroserver-data:/app/data \
  --name metroserver \
  metroserver:latest
```

## Docker Compose

```yaml
---
services:
  metroserver:
    image: ghcr.io/MetrolistGroup/metroserver:latest
    ports:
      - "8080:8080"
    environment:
      - PORT=8080
      - UA_POLICY_FILE=/config/ua_policy.json
      - DATABASE_FILE=/app/data/metroserver.db
      - UA_ADMIN_TOKEN=${UA_ADMIN_TOKEN}
    volumes:
      - ./ua_policy.example.json:/config/ua_policy.json:ro
      - user-agent-data:/app/data
    healthcheck:
      test:
        [
          "CMD",
          "wget",
          "--no-verbose",
          "--tries=1",
          "--spider",
          "http://localhost:8080/health",
        ]
      interval: 30s
      timeout: 3s
      retries: 3
      start_period: 5s
    restart: unless-stopped

volumes:
  user-agent-data:
```
