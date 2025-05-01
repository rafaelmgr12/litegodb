# LiteGoDB
![Go Version](https://img.shields.io/badge/go-1.23-blue.svg)
![License](https://img.shields.io/github/license/rafaelmgr12/litegodb)
![Build](https://github.com/rafaelmgr12/litegodb/actions/workflows/release.yml/badge.svg)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)

LiteGoDB is a lightweight key-value database written in Go, featuring a B-Tree storage engine, write-ahead logging (WAL), SQL-like command support, and a REST/WebSocket interface.

➡️ See the [ROADMAP](./ROADMAP.md) for upcoming features

## Features

- B-Tree-based key-value storage engine
- Write-Ahead Logging (WAL) for durability and crash recovery
- SQL-like query support: `INSERT`, `SELECT`, `DELETE`
- REST API and WebSocket interface
- Native Go client
- CLI client (`litegodbc`)
- Docker-ready for local or containerized deployment

## Getting Started

### Run with Docker

```bash
git clone https://github.com/rafaelmgr12/litegodb.git
cd litegodb
docker compose up --build
```

This will start the LiteGoDB server at http://localhost:8080

## API Usage

### Insert a key-value pair

```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"query":"INSERT INTO users VALUES (1, '\''rafael'\'')"}'
```

### Retrieve a value by key

```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT * FROM users WHERE `key` = 1"}'
```

## Native Go Usage

```go
import "github.com/rafaelmgr12/litegodb/pkg/litegodb"

db, _ := litegodb.Open("config.yaml")
db.Put("users", 1, "rafael")
value, found, _ := db.Get("users", 1)
```

## Testing

Run all unit and integration tests:

```bash
go test ./...
```

## CLI (litegodbc)

The CLI client connects to a LiteGoDB server via HTTP.

```bash
go run cmd/litegodbc/main.go --url http://localhost:8080
> INSERT INTO users VALUES (1, 'joao');
> SELECT * FROM users WHERE `key` = 1;
```

## Project Structure

```
litegodb/
├── cmd/
│   ├── server/        # REST/WebSocket server entrypoint
│   └── litegodbc/     # CLI client
├── internal/
│   └── storage/       # B-Tree engine, disk manager, WAL
├── pkg/
│   └── litegodb/      # Public Go API interface
├── config.yaml        # Server configuration
├── Dockerfile         # Container build config
└── docker-compose.yml # Local dev environment
```

## Contributing

Contributions are welcome! Feel free to fork the repository, submit issues, or open pull requests.

## License

MIT License — see the LICENSE file for details.

