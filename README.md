# Resolve - Entity Matching System

Resolve is a production-grade approximate entity matching system that uses vector embeddings and Qdrant to provide high-quality fuzzy matching and identity resolution at scale.

## Features

- Modular embedding interface (pluggable embedding backends)
- Pluggable similarity threshold (default: 0.85 cosine)
- Index management for Qdrant collections (create/update/delete)
- CLI Commands:
  - `resolve ingest file.csv` – batch ingestion
  - `resolve match "Acme Corp"` – single match
  - `resolve serve` – start HTTP API server with `/match` and `/ingest`
- Match explanations (e.g., normalized input, embedding score)

## Installation

### Prerequisites

1. Go 1.18 or later
2. Qdrant running locally or remotely
3. Python embedding service (local HTTP API)

### Building from Source

```bash
git clone https://github.com/TFMV/resolve.git
cd resolve
go build
```

## Configuration

Resolve can be configured using environment variables or a config file. The default configuration file is `./resolve.yaml`.

Example configuration:

```yaml
# Qdrant Configuration
QDRANT_HOST: localhost
QDRANT_PORT: 6334
QDRANT_API_KEY: ""
QDRANT_USE_TLS: false

# Embedding Service Configuration
EMBEDDING_SERVICE_URL: http://localhost
EMBEDDING_SERVICE_PORT: 8000

# Matching Configuration
SIMILARITY_THRESHOLD: 0.85
COLLECTION_NAME: "entities"
VECTOR_SIZE: 768

# Server Configuration
SERVER_PORT: 8080
```

## Usage

### CLI

#### Entity Ingestion

Ingest entities from a CSV file:

```bash
resolve ingest --format=csv --create-collection --id-column=id --text-column=text testdata/sample.csv
```

Ingest entities from a JSON file:

```bash
resolve ingest --format=json entities.json
```

#### Entity Matching

Match a single entity:

```bash
resolve match "Acme Corporation"
```

Match with detailed output:

```bash
resolve match --details --format=json "Acme Corp"
```

### HTTP API

Start the HTTP API server:

```bash
resolve serve
```

#### API Endpoints

- `GET /` - API info
- `GET /health` - Health check
- `POST /api/match` - Match entities
- `POST /api/ingest` - Ingest entities
- `GET /api/stats` - Get stats

##### Match Endpoint

```json
POST /api/match
{
  "text": "Acme Corp",
  "limit": 10,
  "threshold": 0.8,
  "include_details": true
}
```

##### Ingest Endpoint

```json
POST /api/ingest
{
  "entities": [
    {
      "id": "ent1",
      "original_text": "Acme Corporation"
    },
    {
      "id": "ent2",
      "original_text": "Apple Inc."
    }
  ]
}
```

## Architecture

Resolve is built with a modular architecture:

- `cmd/` - CLI entrypoints
- `api/` - HTTP handlers
- `internal/`
  - `normalize/` - Input normalization
  - `embed/` - HTTP wrapper to Python embedding service
  - `qdrant/` - Qdrant Go client wrapper
  - `match/` - Core matching logic
- `config/` - Config loader
- `testdata/` - Test data

## Development

### Testing

Run tests:

```bash
go test ./...
```

### Python Embedding Service

The Python embedding service should expose an HTTP endpoint at `/embed` that accepts POST requests with the following format:

```json
{
  "texts": ["text1", "text2", "text3"]
}
```

And returns:

```json
{
  "embeddings": [[0.1, 0.2, ...], [0.3, 0.4, ...], [0.5, 0.6, ...]]
}
```

## License

[MIT](LICENSE)
