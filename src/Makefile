# Go parameters
BINARY_NAME=go-file-dedupe

# Variables
GO_CMD := go


.PHONY: all build clean fmt help lint test test-clean

all: build

help: ## Show this help message
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the application binary
	@echo "Building $(BINARY_NAME)..."
	@go build -o $(BINARY_NAME) .


fmt: ## Format the Go source code.
	$(GO_CMD) fmt ./...

vet: ## Run go vet to check for suspicious constructs.
	$(GO_CMD) vet ./...

lint: ## Run the linter on the codebase.
	revive ./... | grep -v _test.go || true

check: fmt vet lint ## Run all code quality checks (format, vet, lint).

test: ## Run tests with the race detector enabled.
	$(GO_CMD) test -v -race ./...

test-clean: ## Clear the test cache.
	$(GO_CMD) clean -testcache

clean: ## Remove build artifacts
	@echo "Cleaning..."
	@if [ -f "$(BINARY_NAME)" ]; then rm $(BINARY_NAME); fi
