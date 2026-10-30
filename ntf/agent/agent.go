// Copyright 2026 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package agent

import (
	"context"
	"log/slog"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/backend"
	"github.com/synadia-io/orbit.go/ntf/events"
)

// Capability is something the agents of a Provider can do. A provider declares
// its capabilities through Provider.Capabilities, and the service refuses a
// create that needs one the selected provider does not declare.
type Capability string

const (
	// CapabilityCapture means the provider's agents implement ClientEdge,
	// fronting each managed server's client listener with a capture proxy
	// and, when Spec.Store is true, keeping the captures of the connections
	// through it. Creates with trace or proxy need it.
	CapabilityCapture Capability = "capture"

	// CapabilityShaping means Agent.Shape, ClearShaping, ShapingReport and
	// ShapingSets apply and report shaping sets rather than returning
	// ErrUnsupported.
	CapabilityShaping Capability = "shaping"

	// CapabilityToxics means the agent runs a proxy for each of Spec.Links
	// and, through ClientEdge.Front, each of Spec.ClientProxies, and faults
	// the links of an instance through the Toxics interface, which its agents
	// implement. Creates with toxics and the toxic calls need it.
	CapabilityToxics Capability = "toxics"
)

// Options is what the service passes a Factory.
//
// Conn, Server and Events are local resources for an agent running in the
// service's process. A remote agent ignores them.
type Options struct {
	// Dir is the service's directory, which holds per-instance state.
	Dir string

	// Preserve keeps the agent's scratch space on Agent.Stop.
	Preserve bool

	// Logger receives the agent's log lines.
	Logger *slog.Logger

	// Conn is the service's management connection.
	Conn *nats.Conn

	// Server is the service's embedded management server, nil when the
	// service was given its own connection.
	Server *server.Server

	// Events publishes the agent's events of an instance. The agent fills
	// instance_id and the fields of the event's type. Events stamps the type,
	// id, timestamp in UTC, source agent and the instance's next source_seq,
	// and publishes on the management connection without waiting for an ack,
	// so an event may be lost and its source_seq then shows a gap.
	Events events.Publisher
}

// Factory builds a provider. ctx limits startup only: the provider runs until
// Provider.Close.
type Factory func(ctx context.Context, opts Options) (Provider, error)

// Provider starts the agents of a service, one for each instance that needs
// one. The service calls Start concurrently, for different instances.
// Capabilities is safe to call at any time.
type Provider interface {
	// Capabilities returns the capabilities the provider's agents declare, in
	// the order status reports them.
	Capabilities() []Capability

	// Start starts an agent for the instance spec describes and returns it.
	// The service calls it before the instance's servers start. A failure to
	// bind a listener wraps ErrListen.
	Start(ctx context.Context, spec Spec) (Agent, error)

	// Close releases what the provider holds for the whole service. The
	// service calls it once, after every instance is gone and before the
	// management server stops.
	Close() error
}

// Spec describes the instance an agent is to serve.
type Spec struct {
	// InstanceID is the instance the agent serves.
	InstanceID string

	// ListenHost is the host the agent binds and advertises. Empty binds all
	// interfaces.
	ListenHost string

	// Store is true when the agent keeps captures, and false when it only
	// forwards and shapes.
	Store bool

	// Links are the route and gateway proxies of an instance created with
	// toxics, which the agent runs from Start, before the servers dial them.
	// Empty for any other instance.
	Links []Link

	// ClientProxies are the client proxies of an instance created with
	// toxics, keyed by the name of the managed server each one fronts, which
	// is the Front.ServerName the service fronts that server with. Each is
	// the endpoint of the proxy as placement returned it; the agent binds its
	// port on ListenHost when the service fronts that server with
	// ClientEdge.Front, never at Start, since the backend may hold the port
	// until then. The server's live client endpoint, the proxy's upstream, is
	// known only once the server runs. Empty for any other instance.
	ClientProxies map[string]backend.Endpoint

	// CaptureProxies are the capture proxies of an instance created with trace
	// or proxy, one for every node, keyed by the name of the managed server
	// each one fronts, which is the Front.ServerName the service fronts that
	// server with. Each is the endpoint of the proxy as placement returned it;
	// the agent binds its port on ListenHost when the service fronts that
	// server with ClientEdge.Front and Front.Capture set, never at Start, since
	// the backend may hold the port until then. A server with no entry gets a
	// capture proxy on a port the OS chooses. Empty for any other instance.
	CaptureProxies map[string]backend.Endpoint

	// Nodes are the nodes of an instance created with toxics, in start
	// order, each with its cluster, which the agent resolves the selectors of
	// Toxics calls against. Empty for any other instance.
	Nodes []Node
}

// Node is one node of an instance.
type Node struct {
	// Name is the node's name, the name its links and client proxy use. It
	// equals the server_name the node's config renders, which a gateway
	// dialer gives in the INFO it sends after its CONNECT, so an agent can
	// tell which node a gateway connection comes from. A custom template that
	// renders another server_name breaks that.
	Name string

	// Cluster is the cluster the node belongs to, empty for a single server.
	Cluster string
}

// Link is a proxy on one link between the nodes of an instance: it listens on
// Listen and forwards every connection it accepts to Upstream.
type Link struct {
	// Name names the link: "route:<node>-><node>" for a route,
	// "gateway:<cluster>-><node>" for a gateway.
	Name string

	// Kind is "route" or "gateway".
	Kind string

	// From is the node that dials through a route proxy, or the cluster whose
	// nodes all dial through a gateway proxy.
	From string

	// To is the node the proxy forwards to.
	To string

	// Listen is the endpoint the proxy listens on, which the nodes' configs
	// dial.
	Listen backend.Endpoint

	// Upstream is the endpoint of To's cluster listener for a route, or of
	// its gateway listener for a gateway.
	Upstream backend.Endpoint
}

// Front describes a managed server ClientEdge.Front is to front.
type Front struct {
	// ServerName is the managed server's name, recorded in captures.
	ServerName string

	// Client is the endpoint of the server's live client listener, which the
	// agent dials.
	Client backend.Endpoint

	// Capture puts a capture proxy in front of the server, on its endpoint in
	// Spec.CaptureProxies: clients dial it, and it forwards to the server's
	// client proxy in Spec.ClientProxies when it has one, or to Client
	// otherwise. False fronts the server with its client proxy alone, which
	// only an instance created with toxics has.
	Capture bool
}

// ClientEdge is the capability to front a managed server's client listener.
// The agents of a provider that declares CapabilityCapture or
// CapabilityToxics implement it. The service calls Front once each server
// runs, before clients learn of it: with Capture set for every server of a
// traced or proxied instance, and, for an instance created with toxics alone,
// for every server with Capture false.
type ClientEdge interface {
	// Front starts forwarding to front.Client and returns the endpoint
	// clients dial instead: the capture proxy's when front.Capture is set,
	// and the client proxy's otherwise. It fails for a server name it already
	// fronts. A failure to bind a listener wraps ErrListen.
	Front(ctx context.Context, front Front) (backend.Endpoint, error)
}

// Toxics is the capability to fault the links of an instance created with
// toxics. The agents of a provider that declares CapabilityToxics implement
// it. Every method takes and returns plain values, so each can become a NATS
// request and reply, and the service calls them concurrently.
//
// A toxic names its links with an api.ToxicSelector, which the agent resolves
// to its proxies and their streams, where the upstream of a proxy carries what
// its dialer sends and the downstream what it receives:
//
//   - On a route, the data from node A to node B is the upstream of the A to B
//     proxy and the downstream of the B to A proxy, since the pooled route
//     connections carry data both ways and either side may have dialed.
//   - On a gateway, every node of the source cluster dials through the proxy to
//     a node of another cluster, and messages travel on the sender's outbound
//     connection only, so the data from cluster C to node N is the upstream of
//     the C to N proxy, and a toxic other than a cut acts on the upstream
//     alone. A gateway toxic whose from is a node acts on its cluster's
//     proxies.
//   - On a client link, the data to the server is the upstream.
//   - A node alone selects every route, gateway and client link of the node,
//     both ways.
//   - A cut always acts on both streams. Where a gateway proxy is shared by
//     nodes the cut does not name, such as the proxies of a node it unplugs,
//     it silences only the named nodes' connections through it, identified by
//     the server_name in the dialer's INFO, which is Node.Name.
//
// A call refused for what it asked returns an error wrapping ErrInvalidToxic
// that names the toxic and the field; any other error is the agent's own
// failure. No call touches anything the agent keeps on its proxies for itself,
// such as the gate of a gateway proxy.
type Toxics interface {
	// AddToxic validates toxic, resolves its selector and applies it to every
	// link the selector names, or to none on failure. Its name must not be in
	// use. It returns the toxic as applied: with its toxicity filled in, the
	// links it resolved to, and when it was added.
	AddToxic(ctx context.Context, toxic api.Toxic) (api.ActiveToxic, error)

	// RemoveToxic removes the toxic named name from every link it was applied
	// to.
	RemoveToxic(ctx context.Context, name string) error

	// ClearToxics removes every toxic and returns their names in the order
	// they were added.
	ClearToxics(ctx context.Context) ([]string, error)

	// ToxicReport returns every proxied link with the toxics on it, routes
	// and gateways in the order of Spec.Links and then client links in the
	// order of Spec.Nodes, and every toxic in place in the order they were
	// added.
	ToxicReport(ctx context.Context) ([]api.LinkToxics, []api.ActiveToxic, error)
}

// ShapingLister is the capability to return the shaping sets of an instance as
// they were submitted, so a caller can read a set, change it and set it again.
// It is optional and no capability declares it: an agent with shaping may
// implement it, and the service forwards tester.shape.list to it, answering
// that shaping is not supported when the agent does not. Its method takes and
// returns plain values, so it can become a NATS request and reply.
type ShapingLister interface {
	// ListShaping returns the set id, or every set when id is empty in the
	// order Agent.ShapingSets returns, each as Agent.Shape was last handed it.
	// An id the agent does not hold is an error, which reaches the caller as
	// is. An agent without shaping returns ErrUnsupported.
	ListShaping(ctx context.Context, id string) ([]api.ShapingSet, error)
}

// Agent is a running network agent serving one instance. Every method takes
// and returns plain values, so it can become a NATS request and reply.
//
// The service calls an agent's methods concurrently, including while Stop runs
// and after it returns.
//
// The shaping methods act on the connections through the servers the agent
// fronts with Front.Capture set, the captured servers. A set is known by its
// id across the instance: one with an empty Server applies on every captured
// server, and one naming a server exists on that server alone. An agent
// without shaping refuses Shape, ClearShaping, ShapingReport and ShapingSets
// with ErrUnsupported.
type Agent interface {
	// Endpoint returns where clients dial instead of the first managed server
	// the agent fronts. It is the zero Endpoint until ClientEdge.Front first
	// returns, and fixed after that.
	Endpoint() backend.Endpoint

	// Shape adds a set on every captured server, or on set.Server alone when
	// it is not empty, replacing one with the same id and resetting its
	// counters. A replaced set leaves the servers the new set does not apply
	// on. Its error names the rule and field it could not apply, or a
	// set.Server that is not a captured server, and reaches the caller as is;
	// nothing changes on an error.
	Shape(ctx context.Context, set api.ShapingSet) error

	// ClearShaping removes the set id, or every set when id is empty, from
	// every server it applies on, and returns the ids removed.
	ClearShaping(ctx context.Context, id string) ([]string, error)

	// ShapingReport returns the firings of the set id, or of every set when
	// id is empty, one report per set, in the order ShapingSets returns.
	// Each report merges the firings on every server the set applies on, in
	// the order of their time, and counts them per server in
	// api.ShapingReport.Servers.
	ShapingReport(ctx context.Context, id string) ([]api.ShapingReport, error)

	// ShapingSets returns the ids of the sets applied, in the order they were
	// first added, which is the order ShapingReport returns for an empty id.
	ShapingSets(ctx context.Context) ([]string, error)

	// Stop returns once the agent stops accepting connections and its
	// captures are flushed, with an error giving the number that failed to
	// store. If ctx ends first it returns the context's error and the flush
	// carries on. It removes the agent's scratch space unless
	// Options.Preserve is set.
	Stop(ctx context.Context) error
}
