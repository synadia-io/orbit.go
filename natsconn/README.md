# natsconn

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/synadia-io/orbit.go/natsconn
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-io/orbit.go/natsconn
[Build-Status-Url]: https://github.com/synadia-io/orbit.go/actions/workflows/natsconn.yaml
[Build-Status-Image]: https://github.com/synadia-io/orbit.go/actions/workflows/natsconn.yaml/badge.svg?branch=main
[GoDoc-Url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/natsconn
[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/natsconn.svg

[![License][License-Image]][License-Url]
[![Go Reference][GoDoc-Image]][GoDoc-Url]
[![Build Status][Build-Status-Image]][Build-Status-Url]
[![Go Report Card][ReportCard-Image]][ReportCard-Url]

`natsconn` provides a flexible connection helper for establishing NATS connections with various authentication methods and configuration options.

## Installation

```bash
go get github.com/synadia-io/orbit.go/natsconn
```

## Usage

The `natsconn` package simplifies NATS connection creation by providing a unified configuration interface that supports multiple authentication methods.

### Basic Connection

```go
package main

import (
    "log"
    "github.com/synadia-io/orbit.go/natsconn"
)

func main() {
    cfg := &natsconn.NatsConnConf{
        NatsServers:        []string{"nats://localhost:4222"},
        NatsConnectionName: "my-service",
    }

    nc, err := natsconn.NewNatsConnection(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()

    log.Println("Connected to NATS!")
}
```

### Using NATS Context

When a NATS context is provided, it takes priority over other configuration options:

```go
cfg := &natsconn.NatsConnConf{
    NatsContext:        "my-context",
    NatsConnectionName: "context-based-connection",
}

nc, err := natsconn.NewNatsConnection(cfg)
if err != nil {
    log.Fatal(err)
}
```

## Authentication Methods

The package supports multiple authentication methods, applied in the following priority order:

### 1. Credentials File

```go
cfg := &natsconn.NatsConnConf{
    NatsServers:         []string{"nats://localhost:4222"},
    NatsCredentialsFile: "/path/to/creds.file",
}
```

### 2. JWT and Seed

```go
cfg := &natsconn.NatsConnConf{
    NatsServers:  []string{"nats://localhost:4222"},
    NatsUserJWT:  "your-jwt",
    NatsUserSeed: "your-seed",
}
```

### 3. NKey Authentication

```go
cfg := &natsconn.NatsConnConf{
    NatsServers:  []string{"nats://localhost:4222"},
    NatsUserNkey: "your-nkey",
    NatsUserSeed: "your-seed",
}
```

### 4. Username and Password

```go
cfg := &natsconn.NatsConnConf{
    NatsServers:      []string{"nats://localhost:4222"},
    NatsUser:         "username",
    NatsUserPassword: "password",
}
```

## TLS Configuration

The package supports comprehensive TLS configuration:

```go
cfg := &natsconn.NatsConnConf{
    NatsServers:  []string{"tls://localhost:4443"},
    NatsTLSCert:  "/path/to/cert.pem",
    NatsTLSKey:   "/path/to/key.pem",
    NatsTLSCA:    "/path/to/ca.pem",
    NatsTLSFirst: true, // Use TLS handshake first mode
}
```

## Configuration Options

The `NatsConnConf` struct provides the following configuration options:

| Field                 | Type            | Description                               |
| --------------------- | --------------- | ----------------------------------------- |
| `NatsContext`         | `string`        | NATS context name (takes priority if set) |
| `NatsServers`         | `[]string`      | List of NATS server URLs                  |
| `NatsUserNkey`        | `string`        | User NKey for authentication              |
| `NatsUserSeed`        | `string`        | User seed for NKey or JWT authentication  |
| `NatsUserJWT`         | `string`        | User JWT for authentication               |
| `NatsUser`            | `string`        | Username for basic authentication         |
| `NatsUserPassword`    | `string`        | Password for basic authentication         |
| `NatsJSDomain`        | `string`        | JetStream domain                          |
| `NatsConnectionName`  | `string`        | Connection name for identification        |
| `NatsCredentialsFile` | `string`        | Path to credentials file                  |
| `NatsTimeout`         | `time.Duration` | Connection timeout                        |
| `NatsTLSCert`         | `string`        | Path to TLS certificate                   |
| `NatsTLSKey`          | `string`        | Path to TLS key                           |
| `NatsTLSCA`           | `string`        | Path to TLS CA certificate                |
| `NatsTLSFirst`        | `bool`          | Use TLS handshake first mode              |

## Error Handling

The package provides typed errors for better error handling:

```go
nc, err := natsconn.NewNatsConnection(cfg)
if err != nil {
    switch {
    case errors.Is(err, natsconn.ErrNilNatsConnConfig):
        log.Fatal("Configuration cannot be nil")
    case errors.Is(err, natsconn.ErrNoNatsServers):
        log.Fatal("No NATS servers provided")
    case errors.Is(err, natsconn.ErrNatsConnectionFailed):
        log.Fatal("Failed to connect to NATS:", err)
    default:
        log.Fatal("Unexpected error:", err)
    }
}
```

## Complete Example

```go
package main

import (
    "log"
    "time"
    "github.com/synadia-io/orbit.go/natsconn"
)

func main() {
    cfg := &natsconn.NatsConnConf{
        NatsServers:         []string{"nats://localhost:4222", "nats://localhost:4223"},
        NatsConnectionName:  "my-application",
        NatsCredentialsFile: "./nats.creds",
        NatsTimeout:         5 * time.Second,
        NatsTLSCA:          "./ca.pem",
    }

    nc, err := natsconn.NewNatsConnection(cfg)
    if err != nil {
        log.Fatal("Failed to connect:", err)
    }
    defer nc.Close()

    // Use the connection
    if err := nc.Publish("test.subject", []byte("Hello NATS!")); err != nil {
        log.Fatal("Failed to publish:", err)
    }

    log.Println("Message published successfully!")
}
```

