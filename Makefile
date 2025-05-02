# Makefile for LiteGoDB

# Variables
GO := go
DOCKER_COMPOSE := docker-compose
BINARY := litegodb
CONFIG := config.yaml

# Default target
.PHONY: all
all: build

# Build the project
.PHONY: build
build:
	$(GO) build -o $(BINARY) ./cmd/server/main.go

# Run tests
.PHONY: test
test:
	$(GO) test ./...

# Run tests with race detection
.PHONY: test-race
test-race:
	$(GO) test -race ./...

# Run the project locally
.PHONY: run
run:
	$(GO) run ./cmd/server/main.go --config $(CONFIG)

# Run the project with Docker Compose
.PHONY: docker-up
docker-up:
	$(DOCKER_COMPOSE) up --build

# Stop Docker Compose
.PHONY: docker-down
docker-down:
	$(DOCKER_COMPOSE) down

# Clean up build artifacts
.PHONY: clean
clean:
	rm -f $(BINARY)

# Format the code
.PHONY: fmt
fmt:
	$(GO) fmt ./...

# Lint the code
.PHONY: lint
lint:
	golangci-lint run

# Generate coverage report
.PHONY: coverage
coverage:
	$(GO) test -coverprofile=cover.out ./...
	$(GO) tool cover -html=cover.out

# Install dependencies
.PHONY: deps
deps:
	$(GO) mod tidy