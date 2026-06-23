# NATS Testing Framework Client

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/synadia-io/orbit.go/ntf-client
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-io/orbit.go/ntf-client
[Build-Status-Url]: https://github.com/synadia-io/orbit.go/actions/workflows/ntf-client.yaml
[Build-Status-Image]: https://github.com/synadia-io/orbit.go/actions/workflows/ntf-client.yaml/badge.svg?branch=main
[GoDoc-Url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/ntf-client
[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/ntf-client.svg

[![License][License-Image]][License-Url]
[![Go Reference][GoDoc-Image]][GoDoc-Url]
[![Build Status][Build-Status-Image]][Build-Status-Url]
[![Go Report Card][ReportCard-Image]][ReportCard-Url]

This is a Go client for the NATS Testing Framework service (`ntf-server`). The service hosts a
[NATS Micro](https://docs.nats.io/using-nats/developer/services) service that creates real NATS servers, clusters,
and super-clusters on demand, and this client drives it from your Go tests.

The point is to move tests out of embedded mode and closer to reality: your tests talk to real servers over real
networks, while staying fast and safe to run in parallel. Each `Create*` call returns an `*Instance` — a handle to a
server, cluster, or super-cluster owned by the calling test — and many instances coexist on one service, so
`inst.Destroy(t)` only tears down what that test created.

## Installation

```bash
go get github.com/synadia-io/orbit.go/ntf-client
```

The import path is `github.com/synadia-io/orbit.go/ntf-client` and the package name is `ntf`:

```go
import ntf "github.com/synadia-io/orbit.go/ntf-client"
```

## Running the service

The service is published as a multi-arch (`linux/amd64`, `linux/arm64`) image at
[`synadia/ntf-server`](https://hub.docker.com/r/synadia/ntf-server). Start it with the `serve` command; the
client connects to its management NATS endpoint (default `nats://127.0.0.1:4222`).

Pin a tag: floating lines such as `:main`, `:2.14`, `:2.12`, or an exact release like `:v1.2.3-nats-2.14.6`. Each
tag bundles a tester version with one nats-server version.

Operator flags:

| Flag                | Env                | Default      | Description                                                                      |
|---------------------|--------------------|--------------|----------------------------------------------------------------------------------|
| `--port=PORT`       | `NATS_PORT`        | `4222`       | Port the management NATS service listens on.                                     |
| `--advertise=HOST`  | `NATS_ADVERTISE`   | _(empty)_    | Host to advertise to clients for discovered servers.                             |
| `--data=DIR`        | `NATS_DATA_DIR`    | _(temp dir)_ | Directory under which per-instance state is stored.                              |
| `--preserve`        | `NATS_PRESERVE`    | _(false)_    | Do not delete instance dirs on shutdown (debugging).                             |
| `--storage-dir=DIR` | `NATS_STORAGE_DIR` | _(temp dir)_ | JetStream storage dir for captured traces (see [Trace capture](#trace-capture)). |

### Network discovery

A cluster or super-cluster allocates each server's client port **randomly at runtime**, so the client must be able to
reach those ports, and the servers must advertise an address the client can dial. This drives how you network the
service:

- **Runner host / native binary** — leave `--advertise` unset. Spawned servers advertise the host's interface IPs,
  reachable from clients on the same host.
- **`docker run --network host`** — set `--advertise=localhost` (or `127.0.0.1`).
- **Job-in-container on the same docker network** — set `--advertise=<service-hostname>` so the job reaches the
  random per-server ports by the service's hostname.

**Bridge networking with `-p` port mapping is not supported for server discovery**: the random per-server ports
cannot be pre-mapped. Use host networking (or a shared docker network) instead.

### GitHub Actions

Run the tester as a [service container](https://docs.github.com/en/actions/use-cases-and-examples/using-containerized-services/about-service-containers)
and run the **job itself in a container**. That puts the job on the same user-defined bridge network as the service,
so it reaches the tester — and the random per-server ports it opens — by the service's hostname, with no port mapping.
`--advertise nats` makes the gossiped cluster addresses (and the generated TLS cert SANs) resolve as `nats` from
inside the job:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    container: golang:1.26-alpine   # the magic: the job joins the services network
    services:
      # the ntf-server service, reachable from the job as host "nats"
      nats:
        image: synadia/ntf-server:2.14.0
        command: serve --advertise nats   # advertise the service hostname
        options: --name nats              # only eases `docker logs nats` when debugging
    steps:
      - run: apk add --no-cache git       # actions/checkout needs git
        shell: sh
      - uses: actions/checkout@v4
      - name: Run tests
        working-directory: ntf-client
        shell: sh
        run: go test -count=1 ./...
        env:
          NATS_TESTER_URL: nats://nats:4222
```

`command: serve` starts the service (the image entrypoint is the tester binary). Running the job in a container is
what makes this work: the per-server client ports are allocated at random and cannot be pre-mapped with bridge `-p`,
but on a shared docker network every container port is reachable by hostname — see
[Network discovery](#network-discovery) above. Read the URL from the environment so the same tests run locally and in
CI:

```go
func testerURL() string {
    if u := os.Getenv("NATS_TESTER_URL"); u != "" {
        return u
    }
    return "nats://localhost:4222"
}
```

To run it locally, host networking keeps the discovered ports reachable on your loopback (so the default
`nats://localhost:4222` works):

```bash
docker run --rm --network host synadia/ntf-server:2.14.0 serve --advertise localhost
```

## Using the client

### Connecting

`ntf.New` connects to the management service and fails the test on error. Pass extra `nats.Option` values if the
management endpoint itself needs auth or TLS:

```go
client := ntf.New(t, "nats://localhost:4222")
```

### Creating a topology

The `WithX` convenience helpers create the instance, connect a NATS client to it, run your callback, and destroy the
instance on the way out — so a whole test is one call:

```go
func TestCreateCluster(t *testing.T) {
    client := ntf.New(t, "nats://localhost:4222")

    client.WithJetStreamCluster(t, 3, func(t *testing.T, nc *nats.Conn, inst *ntf.Instance) {
        if len(inst.Servers) != 3 {
            t.Fatalf("cluster length not correct: %v", len(inst.Servers))
        }

        resp, err := nc.Request("$JS.API.INFO", nil, time.Second)
        if err != nil {
            t.Fatal(err)
        }

        log.Printf("INFO: %s", string(resp.Data))
    })
}
```

The full family, each available with and without JetStream:

| Helper                                           | Creates                                             |
|--------------------------------------------------|-----------------------------------------------------|
| `WithServer` / `WithJetStreamServer`             | a single server                                     |
| `WithCluster` / `WithJetStreamCluster`           | a cluster (`servers >= 2`)                          |
| `WithSuperCluster` / `WithJetStreamSuperCluster` | a super-cluster (`clusters`, `servers` both `>= 2`) |

For more control, call `client.CreateServer` / `CreateCluster` / `CreateSuperCluster` directly and clean up with
`defer inst.Destroy(t)`; you then dial the servers yourself (`inst.RandomServer().URL`, or iterate `inst.Servers`):

```go
inst := client.CreateCluster(t, 3, true)
defer inst.Destroy(t)

nc, err := nats.Connect(inst.RandomServer().URL)
if err != nil {
    t.Fatal(err)
}
defer nc.Close()
```

All of these accept `CreateOption` values: typed snippet helpers (`WithAccounts`, `WithWebSocket`, …) to customize the
rendered config, and `WithConnectOptions(...nats.Option)` to apply auth, TLS, or any client-side option to the
connection the helper opens.

### Parallel tests

Because each instance has its own UUID, storage dir, and auto-generated cluster name, `t.Parallel()` is safe:

```go
for i := range 3 {
    t.Run(fmt.Sprintf("cluster-%d", i), func(t *testing.T) {
        t.Parallel()
        client.WithJetStreamCluster(t, 3, func(t *testing.T, nc *nats.Conn, inst *ntf.Instance) {
            // ...
        })
    })
}
```

Pass `ntf.WithDescription("...")` on a `Create*` call to attach a label that surfaces in `tester.list` /
`tester.status` output — handy for spotting which test owns which instance in the service logs. It defaults to
`t.Name()`.

### Customizing the configuration

Every managed server is rendered from a built-in NATS config template (single account family `USERS1`..`USERS5`,
`$SYS`, optional JetStream, plus the cluster/gateway wiring for the requested topology). Typed snippet helpers drop a
config body into one of the built-in extension points; the service still owns cluster/gateway wiring.

| Helper              | Replaces / adds                                                     |
|---------------------|---------------------------------------------------------------------|
| `WithAccounts`      | replaces the default `USERS1`..`USERS5` / `$SYS` accounts block     |
| `WithSystemAccount` | replaces `system_account: "$SYS"`                                   |
| `WithAuthorization` | replaces `no_auth_user: user1`                                      |
| `WithTLSSnippet`    | top-level TLS block (client TLS) from a caller-supplied body        |
| `WithWebSocket`     | top-level `websocket { … }` block; auto-reserves `.Ports.websocket` |
| `WithMQTT`          | top-level `mqtt { … }` block; auto-reserves `.Ports.mqtt`           |
| `WithLeafNode`      | top-level `leafnodes { … }` block; auto-reserves `.Ports.leafnode`  |
| `WithJetStream`     | extra keys merged inside the built-in `jetstream { }` block         |
| `WithTopLevel`      | free-form top-level lines (limits, debug, max_payload, …)           |

Helpers compose freely — pass several to one `Create*` call. For listener-bearing slots (`websocket`, `mqtt`,
`leafnode`) the service reserves a TCP port and exposes it both in the template env as `.Ports.<slot>` and on
`ManagedServer.Ports["<slot>"]`:

```go
const wsSnippet = `
websocket {
    port: {{ .Ports.websocket }}
    no_tls: true
}
`

inst := client.CreateCluster(t, 2, false, ntf.WithWebSocket(wsSnippet))
defer inst.Destroy(t)

for _, srv := range inst.Servers {
    nc, _ := nats.Connect(fmt.Sprintf("ws://localhost:%d", srv.Ports["websocket"]))
    // …
    nc.Close()
}
```

If you replace `accounts` and remove anonymous access, pass credentials to the helper's `nats.Connect` with
`WithConnectOptions`:

```go
const accountsSnippet = `
accounts {
    EXTRAS {
        users = [ { user: "alice", pass: "wonder" } ]
    }
    $SYS { users = [ { user: "system", pass: "password" } ] }
}
`
const authSnippet = `# auth required, no no_auth_user`

client.WithCluster(t, 2, func(t *testing.T, nc *nats.Conn, inst *ntf.Instance) {
    // nc is connected as alice
}, ntf.WithAccounts(accountsSnippet), ntf.WithAuthorization(authSnippet),
   ntf.WithConnectOptions(nats.UserInfo("alice", "wonder")))
```

`WithJetStream` is the exception to "replace or add a block": its body is **merged inside** the built-in
`jetstream { }` block, so the service keeps emitting `enabled` and `store_dir` and you supply only the extras (e.g.
`domain: HUB`).

### Generated TLS

`WithGeneratedTLS` opts an instance into TLS on its client ports: the service mints a per-instance CA, a server cert
(valid 24h), and — for mutual TLS — a client cert. The CA cert plus client material come back on `Instance.TLS`;
gateways and routes stay plaintext. The convenience helpers auto-wire the returned material into their
`nats.Connect`, so a TLS test is a one-liner:

```go
client.WithCluster(t, 2, func(t *testing.T, nc *nats.Conn, inst *ntf.Instance) {
    // nc is already connected over TLS
}, ntf.WithGeneratedTLS())
```

Defaults: mutual TLS, SANs `["localhost","127.0.0.1","::1"]`. Adjust with TLS sub-options:

```go
ntf.WithGeneratedTLS()                                       // mutual TLS, default SANs
ntf.WithGeneratedTLS(ntf.TLSServerOnly())                    // one-way TLS
ntf.WithGeneratedTLS(ntf.TLSSANs("ci-runner.test", "127.0.0.1")) // caller SANs
ntf.WithGeneratedTLS(ntf.TLSHandshakeFirst())                // TLS before INFO
ntf.WithGeneratedTLS(ntf.TLSTimeout(5 * time.Second))        // handshake timeout
```

When you dial servers yourself (no convenience helper), build a `*tls.Config` from the instance with `ntf.TLSConfig`
and pass `nats.Secure`:

```go
inst := client.CreateCluster(t, 2, false, ntf.WithGeneratedTLS())
defer inst.Destroy(t)

cfg, err := ntf.TLSConfig(inst)
if err != nil {
    t.Fatal(err)
}
nc, err := nats.Connect(inst.RandomServer().URL, nats.Secure(cfg))
```

If you set `TLSHandshakeFirst()`, also pass `nats.TLSHandshakeFirst()` to your own `nats.Connect` or the connection
hangs (the convenience helpers do this for you). `WithGeneratedTLS` is mutually exclusive with `WithTLSSnippet` — reach
for the snippet form only when tests must pin specific certs or ciphers.

### Updating and reloading configuration

A managed server's config changes in two steps, mirroring a real deployment: stage the new config, then reload it.

- `inst.UpdateServer(t, server, opts...)` re-renders the server's config from the supplied snippet/template options
  (the same `With*` helpers used at create time) and writes it to disk. It does **not** signal the server, and works
  whether the server is running or stopped.
- `inst.ReloadServer(t, server)` signals a running server to re-read its on-disk config (`nats-server` `Reload()`).

So a hot-swap is `UpdateServer` then `ReloadServer`; an apply-on-restart is `UpdateServer` then `StopServer` /
`StartServer`:

```go
client.WithServer(t, func(t *testing.T, nc *nats.Conn, inst *ntf.Instance) {
    srv := inst.Servers[0]

    // Stage a new accounts block, then hot-swap it in.
    inst.UpdateServer(t, srv, ntf.WithAccounts(newAccounts))
    inst.ReloadServer(t, srv)
})
```

`UpdateServer` is full-replace: the options you pass are the complete new config, so snippets you don't re-supply are
dropped. The set of port-bearing snippets (`websocket`, `mqtt`, `leafnode`) is fixed at create time — an update that
adds or removes one is rejected; changing the body of an existing one is fine.

### Lifecycle and inspection

Beyond `Destroy`, an instance exposes per-server and whole-instance operations:

- `inst.StopServer(t, srv)` / `inst.StartServer(t, srv)` — stop or revive one node by name (names are globally
  unique), useful for failure-injection tests.
- `inst.Status(t)` — current status of just this instance; `client.Status(t)` and `client.List(t)` report every
  instance.
- `client.Reset(t)` — tear down **every** instance (a CI safety net between stages); prefer `Destroy` for normal
  per-test cleanup.
- `ntf.RandomServer(servers)` and `ntf.RandomClusterServer(cluster, servers)` pick a server to dial.

### Trace capture

`WithTraceCapture` fronts the instance's first server with an in-process capture proxy on a random port. The
convenience helpers still connect directly to the server (untraced); to capture, dial the proxy yourself via
`ManagedServer.TraceURL`. Every connection through the proxy has its trace stored — in the
[Trace Assert](https://github.com/synadia-labs/traceassert) expanded format — as an object in the management server's
`TRACES` JetStream object store.

```go
inst := client.CreateServer(t, true, ntf.WithTraceCapture())
defer inst.Destroy(t)

srv := inst.Servers[0]
nc, err := nats.Connect(srv.TraceURL) // dialed through the proxy
if err != nil {
    t.Fatal(err)
}
// ... traffic on nc is captured ...
nc.Close()
```

Read captures back with `inst.TraceStore(t)`, which returns a standard `jetstream.ObjectStore`. Each object's
metadata (`instance_id`, `server_name`, `client_name`, …) ties it to its origin:

```go
ctx := context.Background()
store := inst.TraceStore(t)
infos, _ := store.List(ctx)
for _, info := range infos {
    if info.Metadata["instance_id"] == inst.ID {
        data, _ := store.GetBytes(ctx, info.Name)
        // data is the expanded format (JSON Lines)
    }
}
```

Trace capture is plaintext-only and is rejected in combination with `WithGeneratedTLS`. On a cluster or super-cluster
only the first node carries a `TraceURL`. The management server enables JetStream lazily — the first time any instance
requests trace capture — so runs that never trace pay no JetStream cost. Captures hold full payloads and headers,
including the CONNECT frame, so treat the `TRACES` store as sensitive.

## Template environment

When you write a snippet body or a full template (`WithTemplate`), it is rendered with Go `text/template` against this
environment. Cluster fields are zero on a single server; super-cluster fields are zero on a single cluster.

| Field            | Type                | Notes                                                                              |
|------------------|---------------------|------------------------------------------------------------------------------------|
| `.ServerName`    | string              | globally unique; `<short>-n<i>` or `<short>-c<c>_s<i>`                             |
| `.ShortID`       | string              | 8-char tail of the instance UUID                                                   |
| `.InstanceID`    | string              | full instance UUID                                                                 |
| `.ServerIndex`   | int                 | 1-based index within the cluster                                                   |
| `.ServerDir`     | string              | absolute path to the server's storage dir                                          |
| `.StoreDir`      | string              | currently equal to `.ServerDir`                                                    |
| `.Host`          | string              | always `localhost` today                                                           |
| `.AdvertiseHost` | string              | `--advertise` value; empty when unset                                              |
| `.ClientPort`    | int                 | client listener port, reserved by the service                                      |
| `.JetStream`     | bool                | mirrors the `jetstream` request field                                              |
| `.ClusterName`   | string              | empty for single-server instances                                                  |
| `.ClusterPort`   | int                 | cluster route listener port                                                        |
| `.Routes`        | []string            | `host:port` strings of every cluster peer                                          |
| `.Gateways`      | map[string][]string | cluster name → gateway URLs (super-cluster only)                                   |
| `.Ports`         | map[string]int      | listener ports keyed by name (auto-populated for WS/MQTT/LeafNode)                 |
| `.Snippets`      | map[string]string   | rendered-snippet paths by name; empty when no snippets passed                      |
| `.TLS`           | struct              | `.TLS.CAFile`/`.CertFile`/`.KeyFile` cert paths; nil unless `WithGeneratedTLS` set |

`WithTemplate` replaces the entire main template with your own body. It composes with the typed helpers (their rendered
files are exposed via `.Snippets.<name>`), but you become responsible for emitting correct `cluster { … }` /
`gateway { … }` wiring — reach for it only when no typed helper expresses the change.

## Service subjects

For reference, the client maps onto these service subjects (useful when adding clients in other languages or for
debugging):

| Subject                       | Description                                                               |
|-------------------------------|---------------------------------------------------------------------------|
| `tester.create.server`        | Creates a single server. Optional `snippets`, `template`, `tls`, `trace`. |
| `tester.create.cluster`       | Creates a cluster.                                                        |
| `tester.create.super-cluster` | Creates a super-cluster.                                                  |
| `tester.stop.server`          | Stops a running server by name.                                           |
| `tester.start.server`         | Starts a previously-stopped server by name.                               |
| `tester.update.server`        | Renders a new on-disk config for a server.                                |
| `tester.reload.server`        | Signals a running server to re-read its on-disk config.                   |
| `tester.status`               | Status of all instances; optional `instance_id` filter.                   |
| `tester.list`                 | Lightweight summary of every instance.                                    |
| `tester.destroy`              | Tears down a single instance by `instance_id`.                            |
| `tester.reset`                | Tears down **every** instance — CI-style global wipe.                     |