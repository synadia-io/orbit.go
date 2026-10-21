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
the `capture` capability. Every node is fronted by a capture proxy of its own, on a trace port from the service's
range that its `ports["trace"]` reports; the first node's is the instance's trace URL for callers that dial one.
Combining either flag with generated TLS is rejected with `013` because the proxy forwards plaintext.

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

An agent serves an instance. `Start` runs before the instance's servers start, with a `Spec` naming the instance,
the host to listen on (the node's `client_advertise`, which must name a local interface), and `Store`, which is
true for `trace` and false for `proxy` alone; the agent keeps captures only when it is true. `Spec.CaptureProxies`
holds each node's trace port, which placement reserves with `ProxyAddress` kind `trace`; the agent binds it when the
node is fronted, and a node with no entry gets a port the OS chooses. Once the servers run, the service fronts every
node through `ClientEdge.Front`, which an agent declaring `capture` implements, with the node's name, its live
client endpoint and `Capture` set, and gets back the endpoint clients reach instead of the managed server's own.
The `Agent` answers the `tester.shape.*` endpoints and is stopped once by the service on teardown and when a create
fails. A `Start` or `Front` that could not bind returns an error wrapping `agent.ErrListen`, and the create runs
again with fresh ports. The agent owns any scratch space it needs.

A shaping set applies on every node's capture proxy, or, with `server` set to a server's name, on that node's alone;
a server that is not captured is refused with `017`. A set's id is unique across the instance, and setting it again
moves it to the nodes the new set names. `tester.shape.report` returns one report per set with the firings of every
node merged in time order and `servers`, the number of firings on each node the set applies on.

## Link proxies

`Create*` requests accept `toxics` to put a proxy of the agent on every route, every gateway between clusters and
every node's client listener, so links can be faulted. The agent must declare the `toxics` capability: with no agent
selected the request is refused with `014`, and with `018` when the agent does not declare it. `Spec.Links` hands
the agent its route and gateway proxies, each with the endpoint it listens on and the peer listener it forwards to,
`Spec.ClientProxies` the endpoint of each node's client proxy, whose upstream is the live client endpoint once
the node runs, and `Spec.Nodes` every node with its cluster, which toxic selectors resolve against. The service
fronts every node through `ClientEdge.Front`, which binds that node's client proxy: with `Capture` false the
agent returns the client proxy's endpoint, and on every node of a traced or proxied instance the capture proxy
forwards to the client proxy, so its chain is client, capture, client proxy, server.

The configs dial the proxies: `.Routes` lists the route proxy for each peer, and `.Gateways` lists, for each other
cluster, the proxies the node's cluster shares to that cluster's nodes. Three template variables, empty without
`toxics`, carry the rest:

| Variable            | Rendered by the built-in template as                                                 |
|---------------------|--------------------------------------------------------------------------------------|
| `.ClientAdvertise`  | `client_advertise`: the `host:port` of the node's client proxy.                      |
| `.ClusterAdvertise` | `advertise` in the `cluster` block: a dead `host:port` that refuses every dial.      |
| `.GatewayAdvertise` | `advertise` in the `gateway` block: the same dead address.                           |

The dead address keeps gossip on the proxies. In-process every node advertises the same dead address, so nats-server
takes a gossiped route to it for a route to itself and never dials it; on a backend where that is not so, a peer that
dials it is refused and drops it. A custom `template` must reference `.ClientAdvertise`, and in a cluster `.Routes`
and `.ClusterAdvertise`, and in a super-cluster `.Gateways` and `.GatewayAdvertise` as well, or the create, and an
update of one of its servers, is refused with `007` naming what is missing. A config that enables TLS on routes or
gateways is refused with `019`, on create and on update, since the gateway proxies read a plain CONNECT; generated
TLS on client ports is allowed. A `tls` inside one entry of the `gateway` block's `gateways` list is refused the same
way, naming the entry. In a super-cluster the `gateway` block's `name` must be the node's cluster name, the only
gateway name its proxies admit, or the create, and an update, is refused with `007` naming the node and the name it
found or that it found none. These checks parse each node's config with its includes as nats-server reads them, and a
config that does not parse is refused with `007`, on create and on update, naming the node and the parse error.

A server's `port` reports its client proxy, and the create response and status list every proxy under `links`, each
with its `name` (`route:<node>-><node>`, `gateway:<cluster>-><node>` or `client:<node>`), `kind`, `from`, `to`, the
`proxy` address it is dialed on (for a client proxy, on the advertise host when one is set) and the `upstream` it
forwards to.

### Faulting links

`tester.toxic.add`, `remove`, `clear` and `report` fault the links of an instance created with `toxics`. The service
forwards each to the agent's `agent.Toxics` interface, which an agent declaring the `toxics` capability implements,
and the agent works out the proxies. `add` takes a `toxic`: a `name` unique on the instance, a `selector`, a `type`,
the `attributes` of that type and a `toxicity`. `remove` takes a `name`, `clear` removes every toxic, and `report`
lists every link with the toxics on it, and every toxic with the `links` its selector resolved to and `since` when
it is in place. Status lists the same toxics under `toxics`. Each change publishes a `toxic_action` event carrying the
toxic, one per toxic `added`, `removed` or `cleared`. Callers' toxics never remove or change what the agent keeps on
its proxies for itself, the gate of a gateway proxy and the cut stages, though cuts act through them.

A selector names what the caller means rather than a proxy: a `kind` (`route`, `gateway` or `client`; empty selects
every kind), `from` and `to` as a node or a cluster, and a `direction`: `to` for the data flowing from `from` toward
`to`, and on a client link toward the server, `from` for the reverse, and `both`, the default. `node` alone selects
every link of one node, both ways, and cannot be combined with `from` or `to`; a `direction` of `to` or `from` needs
`from` or `to`. On a route the data from A to B is the upstream of the A to B proxy and the downstream of the B to A
proxy, since either side may dial a pooled connection. On a gateway it is the upstream of A's proxies to B, since
messages travel on the sender's connection only; the downstream carries interest. Gateway proxies are shared by every
node of their source cluster, so a gateway toxic whose `from` is a node acts on its cluster's proxies.

`type` is toxiproxy's `latency`, `bandwidth`, `slow_close`, `timeout`, `reset_peer`, `slicer` or `limit_data`, with
toxiproxy's attribute names and units, or `cut` or `lossy`. A cut is a blackhole: the proxies keep accepting dials,
data is dropped both ways, and connections go silent until the servers' ping timers close them, as in a partition.
Data already inside a later toxic, such as a latency toxic's buffer, is still delivered after a cut starts. A cut that
names a node also silences that node's connections through the gateway proxies its cluster shares, existing and new,
so a cut of `{"node": "<node>"}` unplugs the node. A cut always acts both ways, and removing it closes the connections
that dropped data while it was in place. `lossy` emulates packet loss as TCP delivers it, as stalls rather than lost
bytes: the stream is cut into 1460-byte segments, each lost with chance `loss`, above 0 and below 1, and a lost
segment stalls the stream for `rto` ms, 200 by default, doubling with each lost retransmission up to `max_rto` ms,
3200 by default, while later data waits behind it. Data that arrives after the stream was idle for 50ms starts a
segment of its own, as a write after a pause goes in a packet of its own. Its attributes are the agent's own, not toxiproxy's. No byte is
lost while it is in place; removing it while the stream after it accepts nothing for more than 5s drops the data it
held, as toxiproxy does when it removes a toxic. `toxicity` is the chance that a toxic acts, 1 by default:
toxiproxy rolls it for each stream of each connection, and again whenever the stream's chain is rebuilt by adding or
removing another toxic. `lossy` rolls it once for each stream of each connection and keeps it across rebuilds. A cut
cuts every connection and accepts no toxicity other than 1.

The toxic calls are refused with `002` without an `instance_id`, `014` when no agent is selected, `018` when the agent
does not declare `toxics`, `404` for an unknown instance, and `020` for an instance created without `toxics`. A toxic
the agent refuses for what it asks, such as a name in use or unknown, a selector that matches no link or names an
unknown node or cluster, a one-way cut, or an unknown attribute or one out of range, is refused with `021` naming the
toxic and the field. Any other failure of the agent answers `022`.

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
| `tester.create.server`        | Creates a single server. Optional `snippets`, `template`, `trace`, `proxy`, `toxics`. |
| `tester.create.cluster`       | Creates a cluster. Optional `snippets`, `template`, `trace`, `proxy`, `toxics`.       |
| `tester.create.super-cluster` | Creates a super-cluster. Optional `snippets`, `template`, `trace`, `proxy`, `toxics`. |
| `tester.stop.server`          | Stops a running server by name (names are globally unique).                 |
| `tester.start.server`         | Starts a previously-stopped server by name.                                 |
| `tester.stop.instance`        | Stops every server in an instance, keeping config and storage.              |
| `tester.start.instance`       | Revives a previously-stopped instance.                                      |
| `tester.update.server`        | Renders a new on-disk config for a server. Optional `snippets`, `template`. |
| `tester.reload.server`        | Signals a running server to re-read its on-disk config.                     |
| `tester.shape.set`            | Adds or replaces a traffic shaping set on the proxies of an instance with them, or on one `server`'s. |
| `tester.shape.clear`          | Removes one shaping set by id, or every set when `set` is empty.            |
| `tester.shape.report`         | Reports the rule firings of one shaping set, or of every set.               |
| `tester.toxic.add`            | Adds a named toxic to the links a selector names, on an instance with `toxics`. |
| `tester.toxic.remove`         | Removes one toxic by `name`.                                                |
| `tester.toxic.clear`          | Removes every toxic of an instance.                                         |
| `tester.toxic.report`         | Lists every link with its toxics, and every toxic with its links.          |
| `tester.status`               | Status of every instance, its `shaping_sets`, `links` and `toxics`; optional `instance_id`. |
| `tester.list`                 | Lightweight summary of every instance (id, kind, servers count).            |
| `tester.destroy`              | Tears down a single instance by `instance_id`.                              |
| `tester.reset`                | Tears down **every** instance — CI-style global wipe.                       |
| `tester.clock`                | Reports each participant's clock against the service host's. Optional `extra` clock subjects. |
| `ntf.events.tester.<id>.instance.*` | Published by the service when an instance is `created` or `destroyed`. |
| `ntf.events.tester.<id>.node.*` | Published by the service when a node is `ready`, `stopped`, `started`, `updated` or `reloaded`. |
| `ntf.events.tester.<id>.shaping.*` | Published by the agent when a shaping set is `applied` or `cleared`, with its `server` when it has one, or a rule `fired`, with the `server_name` it fired on. |
| `ntf.events.tester.<id>.capture.stored` | Published by the agent when a connection's capture is stored. |
| `ntf.events.tester.<id>.toxic.*` | Published by the agent when a toxic is `added` or `removed`, and once per toxic when they are `cleared`. |

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
