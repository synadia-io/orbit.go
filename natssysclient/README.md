# NATS System API Client

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/synadia-io/orbit.go/natssysclient
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-io/orbit.go/natssysclient
[Build-Status-Url]: https://github.com/synadia-io/orbit.go/actions/workflows/natssysclient.yaml
[Build-Status-Image]: https://github.com/synadia-io/orbit.go/actions/workflows/natssysclient.yaml/badge.svg?branch=main
[GoDoc-Url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/natssysclient
[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/natssysclient.svg

[![License][License-Image]][License-Url]
[![Go Reference][GoDoc-Image]][GoDoc-Url]
[![Build Status][Build-Status-Image]][Build-Status-Url]
[![Go Report Card][ReportCard-Image]][ReportCard-Url]

NATS System API Client exposes APIs to interact with the [NATS server monitoring endpoints](https://docs.nats.io/running-a-nats-service/configuration/sys_accounts).

> **Note**: All response structures are compatible with the NATS server v2.10.23 and will be updated to support future additions.

## Installation

```bash
go get github.com/synadia-io/orbit.go/natssysclient
```

## Usage

Each endpoint can be used to get information from either a specific server (by server ID) or from the whole cluster.
When pinging the cluster, the client will wait for a response from each server in the cluster and return the aggregated information.

The client can be configured to wait for a specific timeout for each server to respond (scatter-gather) or a specific number of servers to respond.

```go
// To use the system client, you need to set up a NATS connection using the system account.
nc, err := nats.Connect(nats.DefaultURL, nats.UserInfo("admin", "s3cr3t!"))
if err != nil {
    // handle error
}
defer nc.Close()

sys, err := natssysclient.NewSysClient(nc)
if err != nil {
    // handle error
}

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// get VARZ from a specific server
varz, err := sys.Varz(ctx, "server_id", natssysclient.VarzEventOptions{})
if err != nil {
    // handle error
}
fmt.Println(varz.Varz.Name)

// get VARZ from all servers
varzs, err := sys.VarzPing(ctx, natssysclient.VarzEventOptions{})
if err != nil {
    // handle error
}
for _, v := range varzs {
    fmt.Println(v.Varz.Name)
}
```

### Configuration

When creating the System API Client, you can configure the following options:

- `StallTimer`: Utilized when pinging the cluster, sets the stall timer, aborting the request if a server does not respond within the specified time.
- `ServerCount`: Utilized when pinging the cluster, sets the number of servers to wait for a response from.

The above options are not exclusive, you can set both to wait for a specific number of servers to respond within a specific time frame.

### Pagination

The client provides pagination helpers for endpoints that support it (Connz, Subsz, and Jsz) using Go's `iter` package. These helpers automatically handle pagination, allowing you to iterate over all results without manually managing offsets.

Example for Connz:
```go
// Iterate over all connections from a specific server
for resp, err := range sys.AllConnz(ctx, serverID, natssysclient.ConnzEventOptions{
    ConnzOptions: natssysclient.ConnzOptions{
        Limit: 100, // Page size
    },
}) {
    if err != nil {
        // handle error
    }
    // Process connections in this page
    for _, conn := range resp.Connz.Conns {
        fmt.Printf("Connection ID: %d\n", conn.Cid)
    }
}
```

**Note**: For Jsz, pagination only applies when `Accounts: true` is set. The total number of pages is determined by the `Accounts` field in the JetStreamStats.

#### Concurrent Server Processing

The ping methods return separate iterators for each server, enabling concurrent processing:

```go
serverIterators, err := sys.AllConnzPing(ctx, opts)
if err != nil {
    // handle error
}

// Process servers concurrently
var wg sync.WaitGroup
for i, iter := range serverIterators {
    wg.Add(1)
    go func(serverIndex int, serverIter iter.Seq2[*natssysclient.ConnzResp, error]) {
        defer wg.Done()
        for resp, err := range serverIter {
            if err != nil {
                log.Printf("Server %d error: %v", serverIndex, err)
                return
            }
            // Process server response
            fmt.Printf("Server %s: %d connections\n", resp.Server.ID, len(resp.Connz.Conns))
        }
    }(i, iter)
}
wg.Wait()
```
