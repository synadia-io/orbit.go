# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All commands must be run from a module directory (e.g., `cd kvcodec`).

### Testing
```bash
# Run all tests (unit + integration) for a module
go test -v -count=1 ./...
go test -v -count=1 ./test/...

# Run specific test
go test -v -run TestSpecificFunction

# Combined coverage
go test ./... -coverprofile=coverage.out && go tool cover -html=coverage.out
```

### Linting
```bash
# Full lint suite (matches CI)
go fmt ./...
go vet ./...
go vet ./test/...
staticcheck ./...
staticcheck ./test/...
find . -type f -name "*.go" | xargs misspell -error -locale US
```

Requires `staticcheck` and `misspell`:
```bash
go install honnef.co/go/tools/cmd/staticcheck@latest
go install github.com/client9/misspell/cmd/misspell@latest
```

### Module Management
```bash
go mod tidy
```

## Architecture

### Repository Structure
Multi-module Go repository. Each module is an independent Go module with:

- `go.mod` / `go.sum` for dependencies
- `go.work` workspace linking the module and `./test` subdirectory
- `test/` subdirectory with separate `go.mod` for integration tests (isolates heavy deps like NATS server)
- CI workflow in `.github/workflows/`

### Modules

- **kvcodec** — Transparent encoding/decoding wrapper for NATS JetStream KeyValue stores (Base64, AES, Path codecs with wildcard support)
- **jetstreamext** — JetStream extensions: GetBatch, GetLastMsgs, BatchPublisher, FastPublisher
- **natsext** — Core NATS extensions: RequestMany (scatter-gather with iterators)
- **natssysclient** — System API client for monitoring NATS servers (VARZ, CONNZ, etc.)
- **natscontext** — Context propagation and dialer utilities for NATS connections
- **counters** — Distributed counter using JetStream message counting
- **pcgroups** — Partitioned consumer groups (elastic/static/base)

### Key Conventions

- Each module follows independent versioning
- Test modules use `replace` directives pointing to local module (`..`)
- CI runs lint and test jobs independently per module
- Integration tests use an embedded in-memory NATS server

## Coding Guidelines

- Always use modern `for i := range n` syntax instead of `for i := 0; i < n; i++` where applicable
- Use exported `var` blocks for sentinel errors in a dedicated `errors.go` file (see kvcodec, jetstreamext, counters for examples)
- Always use `errors.Is()` when comparing errors in tests
- Imports: stdlib first, blank line, then third-party — alphabetical within each group
- Functional options pattern: `With...()` (e.g., `WithFastPublisherErrorHandler()`)
