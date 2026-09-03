## NATS Testing Framework Go Service Library

A library that runs NATS servers, clusters and super-clusters on demand, so tests can talk to real servers over
real connections instead of embedding one.

It hosts a NATS Micro service. Clients ask it to create an instance, use the servers it reports back, and destroy
the instance when the test ends. A Go client lives in
[orbit.go/ntf-client](https://github.com/synadia-io/orbit.go/tree/main/ntf-client); other languages drive the
[API](#api) directly.

## Running one

The zero value starts an embedded server on an OS-chosen port and keeps instance state in a temporary directory it
removes on `Close`:

```go
svc, err := ntf.New(ctx, ntf.Options{})
if err != nil {
    return err
}
defer svc.Close()

nc := ntfclient.WithJetStreamCluster(t, svc.ClientURL(), 3)
```

Set `Conn` to host the service on a connection you already have. `Close` leaves that connection open, and
`ClientURL` reports the URL it is connected to, so a client reaches the service the same way in both modes.

| Option          | Default           | Description                                                                          |
|-----------------|-------------------|--------------------------------------------------------------------------------------|
| `Conn`          | _(nil)_           | Host on this connection instead of starting a server. Never closed by `Close`.       |
| `Embedded`      | _(see below)_     | Configures the server started when `Conn` is nil.                                    |
| `Dir`           | _(temp dir)_      | Holds per-instance state. An empty value is created and removed by the service.      |
| `Preserve`      | `false`           | Keep instance directories on teardown, for debugging.                                |
| `AdvertiseHost` | _(empty)_         | Host managed servers advertise as `client_advertise`.                                |
| `Logger`        | _(discard)_       | Receives a line per request and per managed server transition.                       |
| `Name`          | `test-management` | The Micro service name.                                                              |
| `Group`         | `tester`          | Subject group every endpoint hangs off. Changing it moves every subject.             |
| `NewCapturer`   | _(nil)_           | Supplies [trace capture](#trace-capture). Without one, `trace` requests are refused. |

`EmbeddedOptions` carries `Port` (zero and `-1` both bind an OS-chosen port), `ServerName`, and `Log`, which writes
the embedded server's own log to stdout. Managed servers are unaffected by `Log`: they always log to a file under
their instance directory.

| Method           | Description                                                      |
|------------------|------------------------------------------------------------------|
| `ClientURL`      | The address clients connect to, in either mode.                  |
| `Port`           | The embedded server's port, or `0` on a supplied connection.     |
| `ManagementConn` | The connection the service answers requests on.                  |
| `EmbeddedServer` | The server the service started, or nil on a supplied connection. |
| `Dir`            | The resolved instance directory, whether supplied or created.    |
| `Reset`          | Tear down every instance and keep serving.                       |
| `Close`          | Stop serving and release everything the service owns.            |

`New` unwinds whatever it started if any step fails, so a non-nil error leaves nothing running. `Close` stops
answering requests, tears down every instance while the connection is still up, closes the capturer, and shuts
down the embedded server.

## Managed servers

Managed servers listen on real TCP ports, which is the point: tests exercise the network rather than an in-memory
shortcut. They bind the wildcard address, so on a developer machine an instance is reachable from the local
network for as long as it lives.

Servers are rendered from a built-in NATS config template; `snippets` inject config into named extension points
and `template` replaces the whole body. `snippets` is a `{key: body}` map keyed by extension-point name
(`accounts`, `websocket`, …), and the service derives listener-port reservations from those keys: when
`websocket`, `mqtt` or `leafnode` is present, a TCP port under the same name is reserved and surfaces on
`ManagedServer.ports[<name>]`.

Server names are prefixed with the instance's short id so the same `n1` / `c1_s1` style names do not collide
between concurrent instances.

## Trace capture

`Create*` requests accept `trace` to front a server's client port with a capture proxy. The service does not
implement capture itself: `Options.NewCapturer` supplies it, and without one those requests are refused with error
code `014`. Only the first node of a cluster or super-cluster is fronted, and combining `trace` with generated TLS
is rejected because the proxy forwards plaintext.

```go
type Capturer interface {
    Capture(ctx context.Context, req CaptureRequest) (CaptureProxy, error)
    Close() error
}
```

`NewCapturer` is called once at the end of `New` with the `*Service` already usable, so an implementation can
reach the management connection, the embedded server and the instance directory. Keep it cheap and do the
expensive setup on the first `Capture`, so a service that never captures never pays for it.

`CaptureRequest` names the instance and server being fronted, the backend address to forward to, the host to
listen on (the node's `client_advertise`, which must name a local interface), and a temp directory the service
creates under the instance. The `CaptureProxy` that comes back reports the port clients reach instead of the
managed server's own, and is stopped by the service on teardown.

## API

| Subject                       | Description                                                                 |
|-------------------------------|-----------------------------------------------------------------------------|
| `tester.create.server`        | Creates a single server. Optional `snippets`, `template`.                   |
| `tester.create.cluster`       | Creates a cluster. Optional `snippets`, `template`.                         |
| `tester.create.super-cluster` | Creates a super-cluster. Optional `snippets`, `template`.                   |
| `tester.stop.server`          | Stops a running server by name (names are globally unique).                 |
| `tester.start.server`         | Starts a previously-stopped server by name.                                 |
| `tester.stop.instance`        | Stops every server in an instance, keeping config and storage.              |
| `tester.start.instance`       | Revives a previously-stopped instance.                                      |
| `tester.update.server`        | Renders a new on-disk config for a server. Optional `snippets`, `template`. |
| `tester.reload.server`        | Signals a running server to re-read its on-disk config.                     |
| `tester.status`               | Status of all instances; optional `instance_id` filter.                     |
| `tester.list`                 | Lightweight summary of every instance (id, kind, servers count).            |
| `tester.destroy`              | Tears down a single instance by `instance_id`.                              |
| `tester.reset`                | Tears down **every** instance — CI-style global wipe.                       |

The subjects above assume the default `Group`. The Go types for these requests and responses live in the `api`
package.

## What embedding this grants

A client that can publish to the service's subjects can supply the whole nats-server configuration for an
instance, through the `template` and `snippets` fields. That is enough to write files anywhere the process can
write, bind any address, and read files back through config includes. The create response hands back the private
key of the generated TLS client certificate, managed servers carry well-known credentials from the built-in
template, and captured traces contain the CONNECT frames of every connection, credentials included. Any client may
destroy any instance; there is no tenancy, quota or rate limit.

Treat running a `Service` as granting code execution on its host. On a shared server, put it in a dedicated
account and scope subject permissions to its group. `Group` namespaces the endpoints but not Micro's own `$SRV`
discovery subjects, so two services on one connection still collide there.
