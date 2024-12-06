# NATS System API Client

NATS System API Client exposes APIs to interact with the [NATS server monitoring endpoints](https://docs.nats.io/running-a-nats-service/configuration/sys_accounts).

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