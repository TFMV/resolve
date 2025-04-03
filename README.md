# Resolve: High-Performance Entity Matching System

Resolve is an approximate entity matching system written in Go. It matches customer and business entities across multiple dimensions (name, address, city, state, zip, phone, email) to unify fragmented records across data silos, identifying semantically similar entities even when data is messy, incomplete, or inconsistent.

## Features

- Semantic similarity using vector embeddings
- Configurable attribute weighting
- Multi-field matching with tunable thresholds
- Support for batch operations and real-time lookups
- Customizable normalization pipeline
- Both CLI and API interfaces

## Architecture

Resolve follows a layered architecture:

1. **Data Access Layer**
   - Vector database client for storage and retrieval (currently supporting Weaviate)
   - Embedding service client for vector generation
   - Configuration loader for system parameters

2. **Core Processing Layer**
   - Normalization engine for text preprocessing
   - Entity transformation pipeline
   - Embedding generation and caching
   - Vector search and ranking

3. **Application Layer**
   - HTTP API for entity operations and matching
   - CLI for batch processing
   - Result formatting and explanation

## Prerequisites

- Go 1.24+
- Weaviate instance (for vector database)
- Embedding service (for generating vector embeddings)

## Setup

### 1. Install Dependencies

```bash
go get github.com/gorilla/mux
go get github.com/spf13/viper
go get github.com/google/uuid
go get github.com/weaviate/weaviate-go-client/v4
go mod download
```

### 2. Configure Vector Database

Ensure you have a Weaviate instance running. You can use Docker to start one:

```bash
docker run -d --name weaviate-resolve \
  -p 8080:8080 \
  -e PERSISTENCE_DATA_PATH=/var/lib/weaviate \
  -e DEFAULT_VECTORIZER_MODULE=none \
  -e ENABLE_MODULES= \
  semitechnologies/weaviate:1.24.1
```

### 3. Set Up Embedding Service

Set up an embedding service that can generate vectors for your entity text:

```bash
# Example using a sentence-transformers based service
git clone https://github.com/example/embedding-service.git
cd embedding-service
pip install -r requirements.txt
python server.py
```

### 4. Configure Resolve

Copy the sample configuration file and edit as needed:

```bash
cp config.yaml.sample config.yaml
```

Edit `config.yaml` to set up your Weaviate connection, API server settings, and embedding service configuration.

## Usage

### API Server

Start the API server:

```bash
go run cmd/api/main.go --config config.yaml
```

### API Endpoints

#### Health Check

```bash
curl http://localhost:8080/health
```

#### Entity Operations

1. **Add an entity:**

```bash
curl -X POST http://localhost:8080/entities \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Acme Corporation",
    "address": "123 Main St",
    "city": "New York",
    "state": "NY",
    "zip": "10001",
    "phone": "+1 555-123-4567",
    "email": "info@acme.com",
    "vector": [0.1, 0.2, ... ] 
  }'
```

2. **Get an entity by ID:**

```bash
curl http://localhost:8080/entities/{id}
```

3. **Update an entity:**

```bash
curl -X PUT http://localhost:8080/entities/{id} \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Acme Corp Updated",
    "vector": [0.1, 0.2, ... ]
  }'
```

4. **Delete an entity:**

```bash
curl -X DELETE http://localhost:8080/entities/{id}
```

5. **Batch add entities:**

```bash
curl -X POST http://localhost:8080/entities/batch \
  -H "Content-Type: application/json" \
  -d '{
    "entities": [
      {
        "name": "Entity 1",
        "vector": [0.1, 0.2, ... ]
      },
      {
        "name": "Entity 2",
        "vector": [0.3, 0.4, ... ]
      }
    ]
  }'
```

6. **Get entity count:**

```bash
curl http://localhost:8080/entities/count
```

#### Entity Matching

```bash
curl -X POST http://localhost:8080/match \
  -H "Content-Type: application/json" \
  -d '{
    "entity": {
      "name": "Acme Corp",
      "vector": [0.1, 0.2, ... ]
    },
    "threshold": 0.7,
    "limit": 10
  }'
```

## Data Models

### EntityRecord

The main model for storing entity information:

```json
{
  "id": "entity-001",
  "name": "Acme Corporation",
  "name_normalized": "acme corporation",
  "address": "123 Main St",
  "address_normalized": "123 main st",
  "city": "New York",
  "city_normalized": "new york",
  "state": "NY",
  "state_normalized": "ny",
  "zip": "10001",
  "zip_normalized": "10001",
  "phone": "+1 555-123-4567",
  "phone_normalized": "+15551234567",
  "email": "info@acme.com",
  "email_normalized": "info@acme.com",
  "created_at": 1649955600,
  "updated_at": 1649955600,
  "vector": [0.1, 0.2, ...],
  "metadata": {
    "source": "CRM",
    "type": "business"
  }
}
```

### MatchResult

The model for entity matching results:

```json
{
  "entity": {
    "id": "entity-001",
    "name": "Acme Corporation",
    "address": "123 Main St",
    "vector": [0.1, 0.2, ...],
    "metadata": {}
  },
  "score": 0.92,
  "distance": 0.08,
  "match_id": "entity-001",
  "matched_on": ["vector"],
  "explanation": "Vector similarity score: 0.92",
  "field_scores": {
    "vector": 0.92
  },
  "metadata": {}
}
```

## Configuration

Resolve can be configured through the `config.yaml` file. Key configuration options include:

### Server Configuration

```yaml
server:
  port: 8080

api:
  host: "0.0.0.0"
  port: 8080
  read_timeout_secs: 30
  write_timeout_secs: 30
  idle_timeout_secs: 60
```

### Vector Database Configuration

```yaml
weaviate:
  host: "localhost:8080"
  scheme: "http"
  api_key: ""
  class_name: "Entity"
```

### Embedding Service Configuration

```yaml
embedding:
  url: "http://localhost:8000"
  batch_size: 32
  timeout: 30
  cache_size: 1000
  model_name: "all-MiniLM-L6-v2"
  embedding_dim: 384
```

### Matching Configuration

```yaml
matching:
  similarity_threshold: 0.85
  default_limit: 10
  field_weights:
    name: 0.4
    address: 0.2
    city: 0.1
    state: 0.05
    zip: 0.05
    phone: 0.1
    email: 0.1
```

### Normalization Configuration

```yaml
normalization:
  enable_stopwords: true
  enable_stemming: true
  enable_lowercase: true
  name_options:
    remove_legal_suffixes: true
    normalize_initials: true
  address_options:
    standardize_abbreviations: true
    remove_apartment_numbers: true
  phone_options:
    e164_format: true
  email_options:
    lowercase_domain: true
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
