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
| `EventsMaxAge`  | `24h`             | How long the events stream keeps an event.                                           |
| `EventsMaxBytes`| 1 GiB             | Size past which the events stream discards its oldest events.                        |
| `Agents`        | _(nil)_           | Network agent factories to register by name, for [trace capture](#trace-capture).    |
| `Agent`         | _(empty)_         | The registered agent to use. Without one, `trace` or `proxy` requests are refused.   |

`EmbeddedOptions` carries `Port` (zero and `-1` both bind an OS-chosen port), `ServerName`, `StoreDir`, the
JetStream store directory, which defaults to `management` under `Dir`, and `Log`, which writes the embedded server's
own log to stdout. Managed servers are unaffected by `Log`: they always log to a file under their instance
directory.

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
answering requests, tears down every instance while the connection is still up, closes the agent provider, and
shuts down the embedded server.

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

`Create*` requests accept `trace` to front a server's client port with a capture proxy that stores every
connection's capture, and `proxy` to front it with the same proxy and store nothing, for shaping without captures.
`trace` implies `proxy`. The service does not implement capture itself: a network agent, defined in package
`agent`, does. `Options.Agents` registers agent factories by name and `Options.Agent` selects one; with none
selected those requests are refused with error code `014`, and with `018` when the selected agent does not declare
the `capture` capability. Only the first node of a cluster or super-cluster is fronted, and combining either flag
with generated TLS is rejected because the proxy forwards plaintext.

```go
type Factory func(ctx context.Context, opts Options) (Provider, error)

type Provider interface {
    Capabilities() []Capability
    Start(ctx context.Context, spec Spec) (Agent, error)
    Close() error
}
```

`Capabilities` declares what the provider's agents do: `capture`, `shaping` and `toxics`, the `Capability*`
constants. Status reports the selected agent's registered name as `agent` and its capabilities as
`agent_capabilities`, beside the backend's `backend` and `capabilities`, and omits both when no agent is selected.

`New` calls the selected factory once, after the management connection and embedded server exist and before the
endpoints start. Its `Options` carry the service directory, `Preserve`, the logger, the management connection and
the embedded server, which is nil when the service runs on a supplied connection.

`Spec` names the instance and server being fronted, the node's client endpoint to dial, the host to listen on
(the node's `client_advertise`, which must name a local interface), and `Store`, which is true for `trace` and
false for `proxy` alone; the agent keeps captures only when it is true. The `Agent` that comes back reports the
endpoint clients reach instead of the managed server's own, answers the `tester.shape.*` endpoints, and is stopped
by the service on teardown. The agent owns any scratch space it needs.

## Clocks

Events from different components are lined up by their timestamps, so a distributed deployment of ntf must run NTP
on every host. `tester.clock` confirms that rather than replacing it. It reports the service host as the reference,
each instance's agent as sharing the service's clock since it runs in the service's process, and, for each subject
in the request's `extra`, that participant's `offset` from the service host with its `round_trip` and an error
`bound` of half the round trip, or the `error` if it does not answer. The reply holds no threshold; the caller
judges it.

A component joins by answering clock requests on its own Micro service: it adds `events.ClockHandler` as the
endpoint named `clock` under a group no other component or replica uses, and callers list that endpoint's
subject in `extra`. A subject answered by several processes reaches one of them per request, mixing their clocks
into one result. The request and reply are the
`io.synadia.ntf.clock.v1.request` and `io.synadia.ntf.clock.v1.reply` types in the `events` package.

## API

| Subject                       | Description                                                                 |
|-------------------------------|-----------------------------------------------------------------------------|
| `tester.create.server`        | Creates a single server. Optional `snippets`, `template`, `trace`, `proxy`. |
| `tester.create.cluster`       | Creates a cluster. Optional `snippets`, `template`, `trace`, `proxy`.       |
| `tester.create.super-cluster` | Creates a super-cluster. Optional `snippets`, `template`, `trace`, `proxy`. |
| `tester.stop.server`          | Stops a running server by name (names are globally unique).                 |
| `tester.start.server`         | Starts a previously-stopped server by name.                                 |
| `tester.stop.instance`        | Stops every server in an instance, keeping config and storage.              |
| `tester.start.instance`       | Revives a previously-stopped instance.                                      |
| `tester.update.server`        | Renders a new on-disk config for a server. Optional `snippets`, `template`. |
| `tester.reload.server`        | Signals a running server to re-read its on-disk config.                     |
| `tester.shape.set`            | Adds or replaces a traffic shaping set on the proxy of an instance with one. |
| `tester.shape.clear`          | Removes one shaping set by id, or every set when `set` is empty.            |
| `tester.shape.report`         | Reports the rule firings of one shaping set, or of every set.               |
| `tester.status`               | Status of every instance and its `shaping_sets`; optional `instance_id`.    |
| `tester.list`                 | Lightweight summary of every instance (id, kind, servers count).            |
| `tester.destroy`              | Tears down a single instance by `instance_id`.                              |
| `tester.reset`                | Tears down **every** instance — CI-style global wipe.                       |
| `tester.clock`                | Reports each participant's clock against the service host's. Optional `extra` clock subjects. |
| `ntf.events.tester.<id>.instance.*` | Published by the service when an instance is `created` or `destroyed`. |
| `ntf.events.tester.<id>.node.*` | Published by the service when a node is `ready`, `stopped`, `started`, `updated` or `reloaded`. |
| `ntf.events.tester.<id>.shaping.*` | Published by the agent when a shaping set is `applied` or `cleared`, or a rule `fired`. |
| `ntf.events.tester.<id>.capture.stored` | Published by the agent when a connection's capture is stored. |

The subjects above assume the default `Group`. The Go types for these requests and responses live in the `api`
package, and the events in the `events` package. The service keeps its group's events in the stream
`NTF_EVENTS_TESTER`.

## What embedding this grants

A client that can publish to the service's subjects can supply the whole nats-server configuration for an
instance, through the `template` and `snippets` fields. That is enough to write files anywhere the process can
write, bind any address, and read files back through config includes. The create response hands back the private
key of the generated TLS client certificate, managed servers carry well-known credentials from the built-in
template, and captured traces contain the CONNECT frames of every connection, credentials included. Any client may
destroy any instance; there is no tenancy, quota or rate limit.

Treat running a `Service` as granting code execution on its host. On a shared server, put it in a dedicated
account and scope subject permissions to its group. The service also needs `ntf.events.<group>.>` for its events
and `$JS.API.>` for their stream. `Group` namespaces the endpoints but not Micro's own `$SRV` discovery subjects, so
two services on one connection still collide there. On an unauthenticated server any client can publish forged
events or delete the events stream.
