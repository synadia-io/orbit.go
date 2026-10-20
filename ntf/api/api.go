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

package api

import (
	"encoding/json"
	"fmt"
	"time"
)

// TLSMode selects the verification posture for managed TLS. Absence of a
// TLSOptions value disables TLS entirely; the modes only apply when TLS is
// requested.
type TLSMode string

const (
	// TLSModeServer is one-way TLS: the server presents a cert signed by the
	// generated CA but does not require a client cert.
	TLSModeServer TLSMode = "server"
	// TLSModeMutual requires the client to present the issued client cert.
	TLSModeMutual TLSMode = "mutual"
)

// TLSOptions opts a managed instance into generated TLS on its client ports.
// Gateways and routes remain plaintext.
type TLSOptions struct {
	// Mode defaults to TLSModeMutual when empty.
	Mode TLSMode `json:"mode,omitempty"`
	// SANs are the Subject Alternative Names to embed in the server leaf.
	// Strings that parse as IPs become IP SANs; everything else is treated
	// as a DNS name. Empty means ["localhost","127.0.0.1","::1"].
	SANs []string `json:"sans,omitempty"`
	// HandshakeFirst makes the server perform the TLS handshake before sending
	// the INFO protocol. Clients must dial with the matching handshake-first
	// option; the convenience helpers wire it automatically.
	HandshakeFirst bool `json:"handshake_first,omitempty"`
	// Timeout is the TLS handshake timeout (the nats-server tls{} `timeout`)
	// in seconds; fractional values are honored (0.0001 = 100us). Zero uses the
	// managed default of 2 seconds. The Go client helpers set this from a
	// time.Duration.
	Timeout float64 `json:"timeout,omitempty"`
}

// TLSMaterial carries the cert material a caller needs to dial a managed
// instance over TLS. ClientCertPEM/ClientKeyPEM are populated only for
// TLSModeMutual; the server's private key never leaves the service.
type TLSMaterial struct {
	CAPEM         string `json:"ca_pem"`
	ClientCertPEM string `json:"client_cert_pem,omitempty"`
	ClientKeyPEM  string `json:"client_key_pem,omitempty"`
}

type CreateServerRequest struct {
	JetStream   bool              `json:"jetstream"`
	Description string            `json:"description,omitempty"`
	Snippets    map[string]string `json:"snippets,omitempty"`
	Template    string            `json:"template,omitempty"`
	TLS         *TLSOptions       `json:"tls,omitempty"`
	// Trace opts the server into a capture proxy on a random port; every connection
	// that reaches it has its trace stored in the TRACES object store. It implies
	// Proxy.
	Trace bool `json:"trace,omitempty"`
	// Proxy fronts the server with the capture proxy and stores no captures, so
	// shaping sets apply without filling the store. When Trace is set, Proxy has
	// no effect.
	Proxy bool `json:"proxy,omitempty"`
	// Toxics puts a proxy of the instance's agent in front of the server's client
	// listener, which Port then reports. It needs an agent that declares the
	// toxics capability.
	Toxics bool `json:"toxics,omitempty"`
}

type CreateClusterRequest struct {
	Servers     int               `json:"servers"`
	JetStream   bool              `json:"jetstream"`
	Description string            `json:"description,omitempty"`
	Snippets    map[string]string `json:"snippets,omitempty"`
	Template    string            `json:"template,omitempty"`
	TLS         *TLSOptions       `json:"tls,omitempty"`
	// Trace fronts the first node of the cluster with a single capture proxy that
	// stores every connection's trace. It implies Proxy.
	Trace bool `json:"trace,omitempty"`
	// Proxy fronts the first node with the capture proxy and stores no captures,
	// so shaping sets apply without filling the store. When Trace is set, Proxy
	// has no effect.
	Proxy bool `json:"proxy,omitempty"`
	// Toxics routes every route between the nodes and every node's clients
	// through proxies of the instance's agent, which Links lists. It needs an
	// agent that declares the toxics capability.
	Toxics bool `json:"toxics,omitempty"`
}

type CreateSuperClusterRequest struct {
	Servers     int               `json:"servers"`
	Clusters    int               `json:"clusters"`
	JetStream   bool              `json:"jetstream"`
	Description string            `json:"description,omitempty"`
	Snippets    map[string]string `json:"snippets,omitempty"`
	Template    string            `json:"template,omitempty"`
	TLS         *TLSOptions       `json:"tls,omitempty"`
	// Trace fronts the first node of the first cluster with a single capture proxy
	// that stores every connection's trace. It implies Proxy.
	Trace bool `json:"trace,omitempty"`
	// Proxy fronts the first node of the first cluster with the capture proxy and
	// stores no captures, so shaping sets apply without filling the store. When
	// Trace is set, Proxy has no effect.
	Proxy bool `json:"proxy,omitempty"`
	// Toxics routes every route, every gateway between the clusters and every
	// node's clients through proxies of the instance's agent, which Links lists.
	// It needs an agent that declares the toxics capability.
	Toxics bool `json:"toxics,omitempty"`
}

type ManagedServer struct {
	Name    string `json:"name"`
	Cluster string `json:"cluster"`
	// Port is the server's client port, or the port of its client proxy for an
	// instance created with toxics.
	Port  int            `json:"port"`
	Ports map[string]int `json:"ports,omitempty"`
	URL   string         `json:"url,omitempty"`
	// Advertise is the configured client_advertise host for this server, or empty
	// when the node advertises nothing. Clients prefer it over the management
	// connection host when building the node's reachable URL.
	Advertise string `json:"advertise,omitempty"`
	Running   bool   `json:"running"`
	// TraceURL is the capture-proxy URL for a server created with trace capture
	// enabled. Set client-side from Ports["trace"]; empty when not traced.
	TraceURL string `json:"trace_url,omitempty"`
	// Version is the server version the node runs, in its backend's own version
	// string. Set by status only; empty when the node could not report it.
	Version string `json:"version,omitempty"`
}

type CreateResponse struct {
	ID          string           `json:"id"`
	Description string           `json:"description,omitempty"`
	Kind        string           `json:"kind"`
	Servers     []*ManagedServer `json:"servers"`
	TLS         *TLSMaterial     `json:"tls,omitempty"`
	// Links lists the proxied links of an instance created with toxics: its
	// routes, then its gateways, then its client proxies. Empty for any other
	// instance.
	Links []Link `json:"links,omitempty"`
}

// Link is one proxied link of an instance created with toxics: a proxy the
// instance's agent runs, and where it forwards what it accepts.
type Link struct {
	// Name names the link: "route:<node>-><node>", "gateway:<cluster>-><node>"
	// or "client:<node>".
	Name string `json:"name"`
	// Kind is "route", "gateway" or "client".
	Kind string `json:"kind"`
	// From is the node that dials through a route proxy, or the cluster whose
	// nodes dial through a gateway proxy. Empty for a client proxy.
	From string `json:"from,omitempty"`
	// To is the node the proxy forwards to.
	To string `json:"to"`
	// Proxy is the host:port the nodes dial a route or gateway proxy on, and
	// the host:port clients dial a client proxy on, which is on the advertise
	// host when one is set, as the server's client_advertise renders it.
	Proxy string `json:"proxy"`
	// Upstream is the host:port the proxy forwards to: To's cluster, gateway or
	// live client listener.
	Upstream string `json:"upstream"`
}

type DestroyRequest struct {
	InstanceID string `json:"instance_id"`
}

type DestroyResponse struct {
	Destroyed bool `json:"destroyed"`
}

type InstanceSummary struct {
	ID          string    `json:"id"`
	Description string    `json:"description,omitempty"`
	Kind        string    `json:"kind"`
	Cluster     string    `json:"cluster,omitempty"`
	Servers     int       `json:"servers"`
	Created     time.Time `json:"created"`
}

type ListResponse struct {
	Instances []InstanceSummary `json:"instances"`
}

type ResetResponse struct {
	Shutdown bool `json:"shutdown"`
}

type StartServerRequest struct {
	Name string `json:"name"`
}

type StopServerRequest struct {
	Name string `json:"name"`
}

type StopServerResponse struct {
	Shutdown bool `json:"shutdown"`
}

type StartServerResponse struct {
	Started bool `json:"started"`
}

// ServerStateResult reports the post-operation state of one managed server in an
// instance-level stop or start. Error is empty on success; a non-empty Error means
// that node could not be transitioned (the operation is best-effort per node).
type ServerStateResult struct {
	Name    string `json:"name"`
	Running bool   `json:"running"`
	Error   string `json:"error,omitempty"`
}

// StopInstanceRequest stops every server in an instance while keeping its config and
// storage on disk so StartInstance can revive it later.
type StopInstanceRequest struct {
	InstanceID string `json:"instance_id"`
}

type StopInstanceResponse struct {
	Servers []ServerStateResult `json:"servers"`
}

// StartInstanceRequest revives a previously stopped instance, restarting each server
// from its persisted config.
type StartInstanceRequest struct {
	InstanceID string `json:"instance_id"`
}

type StartInstanceResponse struct {
	Servers []ServerStateResult `json:"servers"`
}

type StatusRequest struct {
	InstanceID string `json:"instance_id,omitempty"`
}

type InstanceStatus struct {
	ID          string          `json:"id"`
	Description string          `json:"description,omitempty"`
	Kind        string          `json:"kind"`
	Servers     []ManagedServer `json:"servers"`
	// ShapingSets holds the ids of the shaping sets applied to the instance's
	// capture proxy, in the order they were applied. Empty when the instance has
	// no proxy or the capturer does not shape.
	ShapingSets []string `json:"shaping_sets,omitempty"`
	// Backend is the name the backend running the instance's servers was
	// registered under. Its presence is how a client tells a service whose
	// backend declares no capabilities from an older service that reports none.
	Backend string `json:"backend,omitempty"`
	// Capabilities holds the capabilities the instance's backend declares, such
	// as "kill", "pause" and "version-change", in the order it declares them.
	// Empty when it declares none. The agent's are in AgentCapabilities.
	Capabilities []string `json:"capabilities,omitempty"`
	// Agent is the name the service's network agent was registered under.
	// Empty, and omitted, when the service has no agent.
	Agent string `json:"agent,omitempty"`
	// AgentCapabilities holds the capabilities the service's network agent
	// declares, such as "capture" and "shaping", in the order it declares
	// them. Empty when the service has no agent or it declares none.
	AgentCapabilities []string `json:"agent_capabilities,omitempty"`
	// Links lists the proxied links of an instance created with toxics, as the
	// create response did. Empty for any other instance.
	Links []Link `json:"links,omitempty"`
	// EventsSeq is the stream sequence of the latest of the service's own
	// events of the instance in its events stream, read before the rest of the
	// status, so a reader follows the stream after it without a gap. Agent
	// events are not acked and may land on either side of it. Zero, and
	// omitted, when no service event has been recorded, while the instance is
	// being created, and when the service publishes events live only.
	EventsSeq uint64 `json:"events_seq,omitempty"`
}

type StatusResponse struct {
	Instances []InstanceStatus `json:"instances"`
}

type UpdateServerRequest struct {
	Name     string            `json:"name"`
	Snippets map[string]string `json:"snippets,omitempty"`
	Template string            `json:"template,omitempty"`
	// TLSTimeout, when non-nil, sets the managed TLS handshake timeout (seconds)
	// for a server created with generated TLS. Absent means no change. Apply it
	// with a subsequent reload.
	TLSTimeout *float64 `json:"tls_timeout,omitempty"`
}

type UpdateServerResponse struct {
	Updated bool `json:"updated"`
}

type ReloadServerRequest struct {
	Name string `json:"name"`
}

type ReloadServerResponse struct {
	Reloaded bool `json:"reloaded"`
}

// ShapingSet is a named group of traffic shaping rules applied to the connections
// through an instance's capture proxy whose CONNECT name matches ConnectionName, a
// regular expression. Rules are evaluated in order and the first rule that fires
// takes the frame. The types carry no validation: the capturer compiles a set when
// it is applied and refuses one it cannot, naming the rule and field.
type ShapingSet struct {
	ID             string        `json:"id"`
	ConnectionName string        `json:"connection_name"`
	Rules          []ShapingRule `json:"rules"`
}

// ShapingRule pairs a match with an action. Nth fires on the Nth matching frame
// only, Every on each Nth, and Limit caps how often the rule fires; the counters
// are kept per connection. Zero leaves a selector out.
type ShapingRule struct {
	ID     string        `json:"id"`
	Match  ShapingMatch  `json:"match"`
	Action ShapingAction `json:"action"`
	Nth    int           `json:"nth,omitempty"`
	Every  int           `json:"every,omitempty"`
	Limit  int           `json:"limit,omitempty"`
}

// ShapingMatch selects frames. Every field left empty matches any frame; the fields
// given must all match. Direction is "to_server" or "from_server", Verb lists wire
// verbs such as "PUB" or "MSG", and SID matches a SUB, UNSUB or MSG's sid.
type ShapingMatch struct {
	Direction string        `json:"direction,omitempty"`
	Verb      []string      `json:"verb,omitempty"`
	Subject   *SubjectMatch `json:"subject,omitempty"`
	Reply     *SubjectMatch `json:"reply,omitempty"`
	Header    *HeaderMatch  `json:"header,omitempty"`
	Payload   *PayloadMatch `json:"payload,omitempty"`
	SID       string        `json:"sid,omitempty"`
}

// SubjectMatch matches a frame's subject or reply subject one of three ways: Exact
// compares the whole string, Wildcard is a NATS subject filter, and Grammar is a
// traceassert subject grammar whose captures Where constrains by name.
type SubjectMatch struct {
	Exact    string         `json:"exact,omitempty"`
	Wildcard string         `json:"wildcard,omitempty"`
	Grammar  string         `json:"grammar,omitempty"`
	Where    map[string]any `json:"where,omitempty"`
}

// HeaderMatch matches a frame carrying header Name; when Value is set it must
// equal the header's value too.
type HeaderMatch struct {
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
}

// PayloadMatch matches a frame's payload. JSON maps gjson paths to the values they
// must hold, Prefix is bytes the payload must start with, and Empty matches a
// payload of zero length.
type PayloadMatch struct {
	JSON   map[string]any `json:"json,omitempty"`
	Prefix string         `json:"prefix,omitempty"`
	Empty  bool           `json:"empty,omitempty"`
}

const (
	// ShapingDrop discards the frame; the far end never sees it.
	ShapingDrop = "drop"
	// ShapingStall pauses the frame's direction for Duration before forwarding.
	ShapingStall = "stall"
	// ShapingThrottle limits the frame's direction to Rate with a Burst allowance.
	ShapingThrottle = "throttle"
	// ShapingDisconnect closes the connection without an -ERR.
	ShapingDisconnect = "disconnect"
)

// ShapingAction is what a rule does to a frame it fires on. Kind is one of the
// Shaping* constants; Duration is set for a stall as a Go duration string, and Rate
// ("50/s") and Burst for a throttle.
//
// In JSON a drop or disconnect is the bare string, "drop", and a stall or throttle
// is an object keyed by its kind: {"stall": "3s"} and
// {"throttle": {"rate": "50/s", "burst": 10}}. Both forms are accepted on input.
type ShapingAction struct {
	Kind     string
	Duration string
	Rate     string
	Burst    int
}

// shapingThrottle is the wire form of a throttle's parameters.
type shapingThrottle struct {
	Rate  string `json:"rate"`
	Burst int    `json:"burst,omitempty"`
}

func (a ShapingAction) MarshalJSON() ([]byte, error) {
	switch a.Kind {
	case ShapingStall:
		return json.Marshal(map[string]string{ShapingStall: a.Duration})
	case ShapingThrottle:
		return json.Marshal(map[string]shapingThrottle{ShapingThrottle: {Rate: a.Rate, Burst: a.Burst}})
	default:
		return json.Marshal(a.Kind)
	}
}

func (a *ShapingAction) UnmarshalJSON(data []byte) error {
	var kind string
	err := json.Unmarshal(data, &kind)
	if err == nil {
		*a = ShapingAction{Kind: kind}
		return nil
	}

	var obj map[string]json.RawMessage
	err = json.Unmarshal(data, &obj)
	if err != nil {
		return fmt.Errorf("shaping action must be a string or an object: %w", err)
	}
	if len(obj) != 1 {
		return fmt.Errorf("shaping action object must hold exactly one key, got %d", len(obj))
	}

	for kind, raw := range obj {
		switch kind {
		case ShapingStall:
			var duration string
			err = json.Unmarshal(raw, &duration)
			if err != nil {
				return fmt.Errorf("stall duration must be a string: %w", err)
			}
			*a = ShapingAction{Kind: ShapingStall, Duration: duration}

		case ShapingThrottle:
			var t shapingThrottle
			err = json.Unmarshal(raw, &t)
			if err != nil {
				return fmt.Errorf("throttle must be an object with rate and burst: %w", err)
			}
			*a = ShapingAction{Kind: ShapingThrottle, Rate: t.Rate, Burst: t.Burst}

		default:
			return fmt.Errorf("unknown shaping action %q", kind)
		}
	}

	return nil
}

// ShapeSetRequest adds a shaping set to an instance's capture proxy, replacing one
// with the same id and resetting its counters. The instance must have been created
// with trace capture.
type ShapeSetRequest struct {
	InstanceID string     `json:"instance_id"`
	Set        ShapingSet `json:"set"`
}

type ShapeSetResponse struct {
	Set string `json:"set"`
}

// ShapeClearRequest removes the shaping set named by Set, or every set when Set is
// empty.
type ShapeClearRequest struct {
	InstanceID string `json:"instance_id"`
	Set        string `json:"set,omitempty"`
}

type ShapeClearResponse struct {
	Cleared []string `json:"cleared"`
}

// ShapeReportRequest reports the firings of the shaping set named by Set, or of
// every set when Set is empty.
type ShapeReportRequest struct {
	InstanceID string `json:"instance_id"`
	Set        string `json:"set,omitempty"`
}

type ShapeReportResponse struct {
	Sets []ShapingReport `json:"sets"`
}

// ShapingReport lists every time a rule of one set fired.
type ShapingReport struct {
	ID      string          `json:"id"`
	Firings []ShapingFiring `json:"firings"`
}

// ShapingFiring records one rule firing on one frame. FrameID is the frame's id in
// the capture, so an assertion can find the shaped event, and Action is the kind
// that was applied.
type ShapingFiring struct {
	ConnectionUUID string    `json:"connection_uuid"`
	ClientName     string    `json:"client_name"`
	FrameID        string    `json:"frame_id"`
	Rule           string    `json:"rule"`
	Action         string    `json:"action"`
	Time           time.Time `json:"time"`
}

const (
	// ToxicKindRoute selects the route links between the nodes of a cluster.
	ToxicKindRoute = "route"
	// ToxicKindGateway selects the gateway links between the clusters of a
	// super-cluster.
	ToxicKindGateway = "gateway"
	// ToxicKindClient selects the client links in front of the nodes.
	ToxicKindClient = "client"
)

const (
	// ToxicDirectionTo selects the data flowing from From toward To, and on a
	// client link the data flowing toward the server.
	ToxicDirectionTo = "to"
	// ToxicDirectionFrom selects the data flowing from To back toward From, and
	// on a client link the data flowing from the server.
	ToxicDirectionFrom = "from"
	// ToxicDirectionBoth selects the data flowing either way. It is the default.
	ToxicDirectionBoth = "both"
)

const (
	// ToxicCut blackholes the links: the proxies keep their ports and accept
	// dials, data is dropped both ways, and connections go silent until the
	// servers' ping timers close them. It takes no attributes and always acts
	// both ways. Data already inside a later toxic, such as a latency toxic's
	// buffer, is still delivered after a cut starts.
	ToxicCut = "cut"
	// ToxicLatency delays data by latency ms, give or take jitter ms.
	ToxicLatency = "latency"
	// ToxicBandwidth limits data to rate KB/s.
	ToxicBandwidth = "bandwidth"
	// ToxicSlowClose delays the close of a connection by delay ms.
	ToxicSlowClose = "slow_close"
	// ToxicTimeout drops data and closes the connection after timeout ms, or
	// never closes it when timeout is 0.
	ToxicTimeout = "timeout"
	// ToxicResetPeer resets the connection timeout ms after data arrives.
	ToxicResetPeer = "reset_peer"
	// ToxicSlicer cuts data into pieces of average_size bytes, varying by up to
	// size_variation bytes, sent delay microseconds apart.
	ToxicSlicer = "slicer"
	// ToxicLimitData closes the connection once bytes bytes have passed.
	ToxicLimitData = "limit_data"
)

// ToxicSelector names the links of an instance created with toxics that a
// toxic acts on, by what the caller means rather than by the proxies: a link
// kind, the nodes or clusters at their ends, and the direction the data flows
// in. The agent resolves it to its proxies and their streams, and refuses a
// selector that matches no link, names an unknown node or cluster, or
// contradicts itself, such as a Direction of to or from with neither From nor
// To, or a Node with From or To.
//
// It resolves as agent.Toxics describes. On a route the data from A to B is the
// upstream of the A to B proxy and the downstream of the B to A proxy. On a
// gateway it is the upstream of the proxies of A's cluster to B, which every
// node of that cluster shares, so a gateway toxic whose From is a node acts on
// its whole cluster, and only a cut acts on the downstream. On a client link
// the data to the server is the upstream. A Node alone selects every link of
// the node, and a cut of it unplugs the node, silencing its connections through
// the gateway proxies its cluster shares too. A cut always acts both ways.
type ToxicSelector struct {
	// Kind is one of the ToxicKind* constants. Empty selects every kind.
	Kind string `json:"kind,omitempty"`
	// From is a node or a cluster at one end of the links. Empty matches any.
	// A client link has no From.
	From string `json:"from,omitempty"`
	// To is a node or a cluster at the other end of the links, and the server
	// of a client link. Empty matches any.
	To string `json:"to,omitempty"`
	// Node selects every link of one node, both ways. It cannot be combined
	// with From or To. A cut of a node alone unplugs it.
	Node string `json:"node,omitempty"`
	// Direction is one of the ToxicDirection* constants. Empty means both.
	Direction string `json:"direction,omitempty"`
}

// Toxic is a named fault on the links a selector names. Type is one of the
// Toxic* constants. Attributes are those of toxiproxy's toxic of that type,
// by toxiproxy's names and in its units, which the agent validates before it
// applies them. Toxicity is the chance, in [0, 1], that the toxic acts, 1 when
// nil: toxiproxy rolls it for each stream of each connection, and again
// whenever the stream's chain is rebuilt by adding or removing another toxic.
// A cut cuts every connection and accepts no toxicity other than 1.
type Toxic struct {
	Name       string         `json:"name"`
	Selector   ToxicSelector  `json:"selector"`
	Type       string         `json:"type"`
	Attributes map[string]any `json:"attributes,omitempty"`
	Toxicity   *float64       `json:"toxicity,omitempty"`
}

// ActiveToxic is a toxic in place on an instance, with the names of the links
// its selector resolved to, in report order, and when it was added.
type ActiveToxic struct {
	Toxic
	Links []string  `json:"links"`
	Since time.Time `json:"since"`
}

// LinkToxics is one proxied link of an instance and the toxics on it, in the
// order they were added.
type LinkToxics struct {
	Link
	Toxics []LinkToxic `json:"toxics,omitempty"`
}

// LinkToxic is one toxic on one link, as the caller sees it. Direction is the
// data it acts on, relative to the link: ToxicDirectionTo for the data
// flowing from the link's From toward its To, ToxicDirectionFrom for the
// reverse, and ToxicDirectionBoth for both. Node, set only on a gateway link,
// limits a cut to the connections of that source node, since the link's
// proxy is shared by every node of its cluster.
type LinkToxic struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Direction  string         `json:"direction"`
	Node       string         `json:"node,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
	Toxicity   float64        `json:"toxicity"`
	Since      time.Time      `json:"since"`
}

// ToxicAddRequest adds a toxic to an instance created with toxics. Its name
// must not be in use on the instance.
type ToxicAddRequest struct {
	InstanceID string `json:"instance_id"`
	Toxic      Toxic  `json:"toxic"`
}

type ToxicAddResponse struct {
	Toxic ActiveToxic `json:"toxic"`
}

// ToxicRemoveRequest removes the toxic named Name from an instance.
type ToxicRemoveRequest struct {
	InstanceID string `json:"instance_id"`
	Name       string `json:"name"`
}

type ToxicRemoveResponse struct {
	Removed string `json:"removed"`
}

// ToxicClearRequest removes every toxic from an instance.
type ToxicClearRequest struct {
	InstanceID string `json:"instance_id"`
}

// ToxicClearResponse lists the names of the toxics removed, in the order they
// were added.
type ToxicClearResponse struct {
	Cleared []string `json:"cleared"`
}

// ToxicReportRequest reports the links of an instance and the toxics on them.
type ToxicReportRequest struct {
	InstanceID string `json:"instance_id"`
}

// ToxicReportResponse lists every proxied link of an instance with the toxics
// on it, in the order the create response lists the links, and every toxic in
// place, in the order they were added.
type ToxicReportResponse struct {
	Links  []LinkToxics  `json:"links"`
	Toxics []ActiveToxic `json:"toxics"`
}

// ClockRequest asks the service for the clocks of the participants it knows,
// the service itself and each instance's agent, and of each subject in Extra.
// Each extra subject is a participant's clock endpoint, such as a workload
// engine's, which the service probes.
type ClockRequest struct {
	Extra []string `json:"extra,omitempty"`
}

// ClockResponse reports each participant's clock against the service host's
// clock, the service first, then the agents in instance id order, then the
// extra subjects in the order the request listed them. It holds no threshold
// and no verdict; the caller judges the offsets.
type ClockResponse struct {
	Participants []ClockParticipant `json:"participants"`
}

const (
	// ClockService is the kind of the service itself, the reference clock.
	ClockService = "service"
	// ClockAgent is the kind of an instance's network agent.
	ClockAgent = "agent"
	// ClockExtra is the kind of a clock subject the request listed.
	ClockExtra = "extra"
)

// ClockParticipant is one participant's clock. Offset is its clock less the
// service host's, positive when it is ahead, and the true offset lies within
// Bound of it. RoundTrip is the probe's time in flight and Bound half of it.
// The durations are encoded in nanoseconds.
type ClockParticipant struct {
	// Kind is ClockService, ClockAgent or ClockExtra.
	Kind string `json:"kind"`

	// InstanceID is the instance an agent fronts.
	InstanceID string `json:"instance_id,omitempty"`

	// Subject is the clock subject of an extra participant.
	Subject string `json:"subject,omitempty"`

	// Reference is true for the service, whose host's clock every offset is
	// measured against.
	Reference bool `json:"reference,omitempty"`

	// SharedClock is true for a participant that runs in the service's
	// process and so reads the service host's clock. It is reported with a
	// zero offset without being probed.
	SharedClock bool `json:"shared_clock,omitempty"`

	Offset    time.Duration `json:"offset"`
	RoundTrip time.Duration `json:"round_trip"`
	Bound     time.Duration `json:"bound"`

	// Error is why an extra participant could not be measured, such as no
	// reply. Offset, RoundTrip and Bound are zero when it is set.
	Error string `json:"error,omitempty"`
}
