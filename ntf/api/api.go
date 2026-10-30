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
	Mode TLSMode `json:"mode,omitempty" jsonschema_description:"The TLS verification mode, server for one-way TLS or mutual to require a client cert, mutual when empty"`
	// SANs are the Subject Alternative Names to embed in the server leaf.
	// Strings that parse as IPs become IP SANs; everything else is treated
	// as a DNS name. Empty means ["localhost","127.0.0.1","::1"].
	SANs []string `json:"sans,omitempty" jsonschema_description:"Subject Alternative Names for the server cert, IPs become IP SANs and anything else a DNS name, localhost, 127.0.0.1 and ::1 when empty"`
	// HandshakeFirst makes the server perform the TLS handshake before sending
	// the INFO protocol. Clients must dial with the matching handshake-first
	// option; the convenience helpers wire it automatically.
	HandshakeFirst bool `json:"handshake_first,omitempty" jsonschema_description:"Performs the TLS handshake before the server sends INFO, clients must dial handshake first"`
	// Timeout is the TLS handshake timeout (the nats-server tls{} `timeout`)
	// in seconds; fractional values are honored (0.0001 = 100us). Zero uses the
	// managed default of 2 seconds. The Go client helpers set this from a
	// time.Duration.
	Timeout float64 `json:"timeout,omitempty" jsonschema_description:"The TLS handshake timeout in seconds, fractions honored, 2 seconds when zero"`
}

// TLSMaterial carries the cert material a caller needs to dial a managed
// instance over TLS. ClientCertPEM/ClientKeyPEM are populated only for
// TLSModeMutual; the server's private key never leaves the service.
type TLSMaterial struct {
	CAPEM         string `json:"ca_pem" jsonschema_description:"The PEM of the generated CA"`
	ClientCertPEM string `json:"client_cert_pem,omitempty" jsonschema_description:"The PEM of the issued client cert, for mutual TLS only"`
	ClientKeyPEM  string `json:"client_key_pem,omitempty" jsonschema_description:"The PEM of the issued client key, for mutual TLS only"`
}

type CreateServerRequest struct {
	Type        string            `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	JetStream   bool              `json:"jetstream" jsonschema_description:"Enables JetStream on the server"`
	Description string            `json:"description,omitempty" jsonschema_description:"A description of the instance"`
	Snippets    map[string]string `json:"snippets,omitempty" jsonschema_description:"Config snippets by the extension point of the main template they fill"`
	Template    string            `json:"template,omitempty" jsonschema_description:"A custom main config template, the built-in one when empty"`
	TLS         *TLSOptions       `json:"tls,omitempty" jsonschema_description:"Generated TLS on the client port, none when absent"`
	// Trace opts the server into a capture proxy on a trace port of its own,
	// which Ports["trace"] reports; every connection that reaches it has its
	// trace stored in the TRACES object store. It implies Proxy.
	Trace bool `json:"trace,omitempty" jsonschema_description:"Fronts the server with a capture proxy that stores every connection's trace, implies proxy"`
	// Proxy fronts the server with the capture proxy and stores no captures, so
	// shaping sets apply without filling the store. When Trace is set, Proxy has
	// no effect.
	Proxy bool `json:"proxy,omitempty" jsonschema_description:"Fronts the server with a capture proxy that stores no captures"`
	// Toxics puts a proxy of the instance's agent in front of the server's client
	// listener, which Port then reports. It needs an agent that declares the
	// toxics capability.
	Toxics bool `json:"toxics,omitempty" jsonschema_description:"Fronts the server's client listener with a proxy of the agent so toxics can act on it"`
}

type CreateClusterRequest struct {
	Type        string            `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	Servers     int               `json:"servers" jsonschema:"required,minimum=2,maximum=20" jsonschema_description:"The number of servers in the cluster"`
	JetStream   bool              `json:"jetstream" jsonschema_description:"Enables JetStream on the servers"`
	Description string            `json:"description,omitempty" jsonschema_description:"A description of the instance"`
	Snippets    map[string]string `json:"snippets,omitempty" jsonschema_description:"Config snippets by the extension point of the main template they fill"`
	Template    string            `json:"template,omitempty" jsonschema_description:"A custom main config template, the built-in one when empty"`
	TLS         *TLSOptions       `json:"tls,omitempty" jsonschema_description:"Generated TLS on the client ports, none when absent"`
	// Trace fronts every node of the cluster with a capture proxy of its own,
	// on the trace port its Ports["trace"] reports, that stores every
	// connection's trace. It implies Proxy.
	Trace bool `json:"trace,omitempty" jsonschema_description:"Fronts every node with a capture proxy that stores every connection's trace, implies proxy"`
	// Proxy fronts every node with a capture proxy and stores no captures, so
	// shaping sets apply without filling the store. When Trace is set, Proxy
	// has no effect.
	Proxy bool `json:"proxy,omitempty" jsonschema_description:"Fronts every node with a capture proxy that stores no captures"`
	// Toxics routes every route between the nodes and every node's clients
	// through proxies of the instance's agent, which Links lists. It needs an
	// agent that declares the toxics capability.
	Toxics bool `json:"toxics,omitempty" jsonschema_description:"Routes every route and every node's clients through proxies of the agent so toxics can act on them"`
}

type CreateSuperClusterRequest struct {
	Type        string            `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	Servers     int               `json:"servers" jsonschema:"required,minimum=2,maximum=20" jsonschema_description:"The number of servers in each cluster"`
	Clusters    int               `json:"clusters" jsonschema:"required,minimum=2,maximum=10" jsonschema_description:"The number of clusters"`
	JetStream   bool              `json:"jetstream" jsonschema_description:"Enables JetStream on the servers"`
	Description string            `json:"description,omitempty" jsonschema_description:"A description of the instance"`
	Snippets    map[string]string `json:"snippets,omitempty" jsonschema_description:"Config snippets by the extension point of the main template they fill"`
	Template    string            `json:"template,omitempty" jsonschema_description:"A custom main config template, the built-in one when empty"`
	TLS         *TLSOptions       `json:"tls,omitempty" jsonschema_description:"Generated TLS on the client ports, none when absent"`
	// Trace fronts every node of every cluster with a capture proxy of its
	// own, on the trace port its Ports["trace"] reports, that stores every
	// connection's trace. It implies Proxy.
	Trace bool `json:"trace,omitempty" jsonschema_description:"Fronts every node with a capture proxy that stores every connection's trace, implies proxy"`
	// Proxy fronts every node with a capture proxy and stores no captures, so
	// shaping sets apply without filling the store. When Trace is set, Proxy
	// has no effect.
	Proxy bool `json:"proxy,omitempty" jsonschema_description:"Fronts every node with a capture proxy that stores no captures"`
	// Toxics routes every route, every gateway between the clusters and every
	// node's clients through proxies of the instance's agent, which Links lists.
	// It needs an agent that declares the toxics capability.
	Toxics bool `json:"toxics,omitempty" jsonschema_description:"Routes every route, every gateway and every node's clients through proxies of the agent so toxics can act on them"`
}

type ManagedServer struct {
	Name    string `json:"name" jsonschema_description:"The server's name"`
	Cluster string `json:"cluster" jsonschema_description:"The cluster the server is in"`
	// Port is the server's client port, or the port of its client proxy for an
	// instance created with toxics.
	Port  int            `json:"port" jsonschema_description:"The client port, or the port of the client proxy for an instance created with toxics"`
	Ports map[string]int `json:"ports,omitempty" jsonschema_description:"Further ports by listener name, such as cluster and trace"`
	URL   string         `json:"url,omitempty" jsonschema_description:"The URL clients dial"`
	// Advertise is the configured client_advertise host for this server, or empty
	// when the node advertises nothing. Clients prefer it over the management
	// connection host when building the node's reachable URL.
	Advertise string `json:"advertise,omitempty" jsonschema_description:"The configured client_advertise host, empty when the node advertises nothing"`
	Running   bool   `json:"running" jsonschema_description:"Whether the server is running"`
	// TraceURL is the URL of the server's own capture proxy, for every server
	// of an instance created with trace capture enabled. Set client-side from
	// Ports["trace"]; empty when not traced.
	TraceURL string `json:"trace_url,omitempty" jsonschema_description:"The URL of the server's capture proxy, set client-side from the trace port"`
	// Version is the server version the node runs, in its backend's own version
	// string. Set by status only; empty when the node could not report it.
	Version string `json:"version,omitempty" jsonschema_description:"The server version the node runs, set by status only"`
}

type CreateResponse struct {
	Type        string           `json:"type" jsonschema_description:"The type id of the response"`
	ID          string           `json:"id" jsonschema_description:"The instance id"`
	Description string           `json:"description,omitempty" jsonschema_description:"The description of the instance"`
	Kind        string           `json:"kind" jsonschema_description:"The kind of instance, server, cluster or super-cluster"`
	Servers     []*ManagedServer `json:"servers" jsonschema_description:"The servers of the instance"`
	TLS         *TLSMaterial     `json:"tls,omitempty" jsonschema_description:"The cert material to dial the instance over TLS, absent without TLS"`
	// Links lists the proxied links of an instance created with toxics: its
	// routes, then its gateways, then its client proxies. Empty for any other
	// instance.
	Links []Link `json:"links,omitempty" jsonschema_description:"The proxied links of an instance created with toxics, its routes, then its gateways, then its client proxies"`
}

// Link is one proxied link of an instance created with toxics: a proxy the
// instance's agent runs, and where it forwards what it accepts.
type Link struct {
	// Name names the link: "route:<node>-><node>", "gateway:<cluster>-><node>"
	// or "client:<node>".
	Name string `json:"name" jsonschema_description:"The link's name, route:<node>-><node>, gateway:<cluster>-><node> or client:<node>"`
	// Kind is "route", "gateway" or "client".
	Kind string `json:"kind" jsonschema_description:"The link's kind, route, gateway or client"`
	// From is the node that dials through a route proxy, or the cluster whose
	// nodes dial through a gateway proxy. Empty for a client proxy.
	From string `json:"from,omitempty" jsonschema_description:"The node that dials through a route proxy, or the cluster whose nodes dial through a gateway proxy, empty for a client proxy"`
	// To is the node the proxy forwards to.
	To string `json:"to" jsonschema_description:"The node the proxy forwards to"`
	// Proxy is the host:port the nodes dial a route or gateway proxy on, and
	// the host:port clients dial a client proxy on, which is on the advertise
	// host when one is set, as the server's client_advertise renders it.
	Proxy string `json:"proxy" jsonschema_description:"The host:port the nodes or clients dial the proxy on"`
	// Upstream is the host:port the proxy forwards to: To's cluster, gateway or
	// live client listener.
	Upstream string `json:"upstream" jsonschema_description:"The host:port the proxy forwards to"`
}

type DestroyRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance to destroy"`
}

type DestroyResponse struct {
	Type      string `json:"type" jsonschema_description:"The type id of the response"`
	Destroyed bool   `json:"destroyed" jsonschema_description:"Whether the instance was destroyed"`
}

type InstanceSummary struct {
	ID          string    `json:"id" jsonschema_description:"The instance id"`
	Description string    `json:"description,omitempty" jsonschema_description:"The description of the instance"`
	Kind        string    `json:"kind" jsonschema_description:"The kind of instance, server, cluster or super-cluster"`
	Cluster     string    `json:"cluster,omitempty" jsonschema_description:"The cluster name of a cluster instance"`
	Servers     int       `json:"servers" jsonschema_description:"The number of servers in the instance"`
	Created     time.Time `json:"created" jsonschema_description:"When the instance was created"`
}

// ListRequest is the request of the list endpoint, which reads nothing from
// it.
type ListRequest struct {
	Type string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
}

type ListResponse struct {
	Type      string            `json:"type" jsonschema_description:"The type id of the response"`
	Instances []InstanceSummary `json:"instances" jsonschema_description:"Every instance"`
}

// ResetRequest is the request of the reset endpoint, which reads nothing from
// it.
type ResetRequest struct {
	Type string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
}

type ResetResponse struct {
	Type     string `json:"type" jsonschema_description:"The type id of the response"`
	Shutdown bool   `json:"shutdown" jsonschema_description:"Whether every instance was shut down"`
}

type StartServerRequest struct {
	Type string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	Name string `json:"name" jsonschema:"required,minLength=1" jsonschema_description:"The name of the server to start"`
}

type StopServerRequest struct {
	Type string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	Name string `json:"name" jsonschema:"required,minLength=1" jsonschema_description:"The name of the server to stop"`
}

type StopServerResponse struct {
	Type     string `json:"type" jsonschema_description:"The type id of the response"`
	Shutdown bool   `json:"shutdown" jsonschema_description:"Whether the server shut down"`
}

type StartServerResponse struct {
	Type    string `json:"type" jsonschema_description:"The type id of the response"`
	Started bool   `json:"started" jsonschema_description:"Whether the server started"`
}

// ServerStateResult reports the post-operation state of one managed server in an
// instance-level stop or start. Error is empty on success; a non-empty Error means
// that node could not be transitioned (the operation is best-effort per node).
type ServerStateResult struct {
	Name    string `json:"name" jsonschema_description:"The server's name"`
	Running bool   `json:"running" jsonschema_description:"Whether the server is running after the operation"`
	Error   string `json:"error,omitempty" jsonschema_description:"Why the server could not be stopped or started, empty on success"`
}

// StopInstanceRequest stops every server in an instance while keeping its config and
// storage on disk so StartInstance can revive it later.
type StopInstanceRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance to stop"`
}

type StopInstanceResponse struct {
	Type    string              `json:"type" jsonschema_description:"The type id of the response"`
	Servers []ServerStateResult `json:"servers" jsonschema_description:"The state of each server after the stop"`
}

// StartInstanceRequest revives a previously stopped instance, restarting each server
// from its persisted config.
type StartInstanceRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance to start"`
}

type StartInstanceResponse struct {
	Type    string              `json:"type" jsonschema_description:"The type id of the response"`
	Servers []ServerStateResult `json:"servers" jsonschema_description:"The state of each server after the start"`
}

type StatusRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id,omitempty" jsonschema_description:"The instance to report on, every instance when empty"`
}

type InstanceStatus struct {
	ID          string          `json:"id" jsonschema_description:"The instance id"`
	Description string          `json:"description,omitempty" jsonschema_description:"The description of the instance"`
	Kind        string          `json:"kind" jsonschema_description:"The kind of instance, server, cluster or super-cluster"`
	Servers     []ManagedServer `json:"servers" jsonschema_description:"The servers of the instance"`
	// ShapingSets holds the ids of the shaping sets applied to the instance's
	// capture proxies, on every server or on one, in the order they were first
	// applied. Empty when the instance has no proxy or the capturer does not
	// shape.
	ShapingSets []string `json:"shaping_sets,omitempty" jsonschema_description:"The ids of the shaping sets applied to the instance's capture proxies, in the order they were first applied"`
	// Backend is the name the backend running the instance's servers was
	// registered under. Its presence is how a client tells a service whose
	// backend declares no capabilities from an older service that reports none.
	Backend string `json:"backend,omitempty" jsonschema_description:"The name of the backend running the instance's servers"`
	// Capabilities holds the capabilities the instance's backend declares, such
	// as "kill", "pause" and "version-change", in the order it declares them.
	// Empty when it declares none. The agent's are in AgentCapabilities.
	Capabilities []string `json:"capabilities,omitempty" jsonschema_description:"The capabilities the backend declares, such as kill, pause and version-change"`
	// Agent is the name the service's network agent was registered under.
	// Empty, and omitted, when the service has no agent.
	Agent string `json:"agent,omitempty" jsonschema_description:"The name of the service's network agent, absent when it has none"`
	// AgentCapabilities holds the capabilities the service's network agent
	// declares, such as "capture" and "shaping", in the order it declares
	// them. Empty when the service has no agent or it declares none.
	AgentCapabilities []string `json:"agent_capabilities,omitempty" jsonschema_description:"The capabilities the agent declares, such as capture and shaping"`
	// Links lists the proxied links of an instance created with toxics, as the
	// create response did. Empty for any other instance.
	Links []Link `json:"links,omitempty" jsonschema_description:"The proxied links of an instance created with toxics"`
	// Toxics lists the toxics in place on the links of an instance created
	// with toxics, in the order they were added, each with the links its
	// selector resolved to and when it was added, as tester.toxic.report
	// lists them. Empty when none is in place, for any other instance, and
	// when the agent could not report them.
	Toxics []ActiveToxic `json:"toxics,omitempty" jsonschema_description:"The toxics in place on the links of an instance created with toxics, in the order they were added"`
	// EventsSeq is the stream sequence of the latest of the service's own
	// events of the instance in its events stream, read before the rest of the
	// status, so a reader follows the stream after it without a gap. Agent
	// events are not acked and may land on either side of it. Zero, and
	// omitted, when no service event has been recorded, while the instance is
	// being created, and when the service publishes events live only.
	EventsSeq uint64 `json:"events_seq,omitempty" jsonschema_description:"The stream sequence of the latest service event of the instance, absent when there is none"`
}

type StatusResponse struct {
	Type      string           `json:"type" jsonschema_description:"The type id of the response"`
	Instances []InstanceStatus `json:"instances" jsonschema_description:"The status of each instance reported"`
}

type UpdateServerRequest struct {
	Type     string            `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	Name     string            `json:"name" jsonschema:"required,minLength=1" jsonschema_description:"The name of the server to update"`
	Snippets map[string]string `json:"snippets,omitempty" jsonschema_description:"Config snippets by the extension point of the main template they fill"`
	Template string            `json:"template,omitempty" jsonschema_description:"A custom main config template, the built-in one when empty"`
	// TLSTimeout, when non-nil, sets the managed TLS handshake timeout (seconds)
	// for a server created with generated TLS. Absent means no change. Apply it
	// with a subsequent reload.
	TLSTimeout *float64 `json:"tls_timeout,omitempty" jsonschema_description:"The TLS handshake timeout in seconds of a server created with generated TLS, unchanged when absent"`
}

type UpdateServerResponse struct {
	Type    string `json:"type" jsonschema_description:"The type id of the response"`
	Updated bool   `json:"updated" jsonschema_description:"Whether the server's config was updated"`
}

type ReloadServerRequest struct {
	Type string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	Name string `json:"name" jsonschema:"required,minLength=1" jsonschema_description:"The name of the server to reload"`
}

type ReloadServerResponse struct {
	Type     string `json:"type" jsonschema_description:"The type id of the response"`
	Reloaded bool   `json:"reloaded" jsonschema_description:"Whether the server reloaded its config"`
}

// ShapingSet is a named group of traffic shaping rules applied to the connections
// through an instance's capture proxies whose CONNECT name matches ConnectionName, a
// regular expression. Rules are evaluated in order and the first rule that fires
// takes the frame. The types carry no validation: the capturer compiles a set when
// it is applied and refuses one it cannot, naming the rule and field.
//
// Server, when set, names the one server of the instance whose capture proxy the
// set applies on, a ManagedServer.Name; a server that is not captured is refused.
// Empty applies the set on every server's capture proxy. The id is unique across
// the instance either way.
type ShapingSet struct {
	ID             string        `json:"id" jsonschema_description:"The set's id, unique across the instance"`
	ConnectionName string        `json:"connection_name" jsonschema_description:"A regular expression the CONNECT name of a connection must match"`
	Server         string        `json:"server,omitempty" jsonschema_description:"The one server whose capture proxy the set applies on, every server when empty"`
	Rules          []ShapingRule `json:"rules" jsonschema_description:"The rules, evaluated in order, the first that fires takes the frame"`
}

// ShapingRule pairs a match with an action. Nth fires on the Nth matching frame
// only, Every on each Nth, and Limit caps how often the rule fires; the counters
// are kept per connection. Zero leaves a selector out.
type ShapingRule struct {
	ID     string        `json:"id" jsonschema_description:"The rule's id"`
	Match  ShapingMatch  `json:"match" jsonschema_description:"Selects the frames the rule acts on"`
	Action ShapingAction `json:"action" jsonschema_description:"What the rule does to a frame it fires on"`
	Nth    int           `json:"nth,omitempty" jsonschema_description:"Fires on the Nth matching frame only, left out when zero"`
	Every  int           `json:"every,omitempty" jsonschema_description:"Fires on every Nth matching frame, left out when zero"`
	Limit  int           `json:"limit,omitempty" jsonschema_description:"Caps how often the rule fires, left out when zero"`
}

// ShapingMatch selects frames. Every field left empty matches any frame; the fields
// given must all match. Direction is "to_server" or "from_server", Verb lists wire
// verbs such as "PUB" or "MSG", and SID matches a SUB, UNSUB or MSG's sid.
type ShapingMatch struct {
	Direction string        `json:"direction,omitempty" jsonschema_description:"The direction of the frame, to_server or from_server, either when empty"`
	Verb      []string      `json:"verb,omitempty" jsonschema_description:"Wire verbs the frame must have one of, such as PUB or MSG"`
	Subject   *SubjectMatch `json:"subject,omitempty" jsonschema_description:"Matches the frame's subject"`
	Reply     *SubjectMatch `json:"reply,omitempty" jsonschema_description:"Matches the frame's reply subject"`
	Header    *HeaderMatch  `json:"header,omitempty" jsonschema_description:"Matches a header of the frame"`
	Payload   *PayloadMatch `json:"payload,omitempty" jsonschema_description:"Matches the frame's payload"`
	SID       string        `json:"sid,omitempty" jsonschema_description:"Matches the sid of a SUB, UNSUB or MSG"`
}

// SubjectMatch matches a frame's subject or reply subject one of three ways: Exact
// compares the whole string, Wildcard is a NATS subject filter, and Grammar is a
// traceassert subject grammar whose captures Where constrains by name.
type SubjectMatch struct {
	Exact    string         `json:"exact,omitempty" jsonschema_description:"The whole subject"`
	Wildcard string         `json:"wildcard,omitempty" jsonschema_description:"A NATS subject filter"`
	Grammar  string         `json:"grammar,omitempty" jsonschema_description:"A traceassert subject grammar"`
	Where    map[string]any `json:"where,omitempty" jsonschema_description:"Constraints on the captures of the grammar, by name"`
}

// HeaderMatch matches a frame carrying header Name; when Value is set it must
// equal the header's value too.
type HeaderMatch struct {
	Name  string `json:"name" jsonschema_description:"The header the frame must carry"`
	Value string `json:"value,omitempty" jsonschema_description:"The value the header must hold, any when empty"`
}

// PayloadMatch matches a frame's payload. JSON maps gjson paths to the values they
// must hold, Prefix is bytes the payload must start with, and Empty matches a
// payload of zero length.
type PayloadMatch struct {
	JSON   map[string]any `json:"json,omitempty" jsonschema_description:"gjson paths and the values they must hold"`
	Prefix string         `json:"prefix,omitempty" jsonschema_description:"Bytes the payload must start with"`
	Empty  bool           `json:"empty,omitempty" jsonschema_description:"Matches a payload of zero length"`
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

// ShapeSetRequest adds a shaping set to an instance's capture proxies, every
// server's or the one Set.Server names, replacing one with the same id and
// resetting its counters. The instance must have been created with trace capture.
type ShapeSetRequest struct {
	Type       string     `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string     `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance whose capture proxies get the set"`
	Set        ShapingSet `json:"set" jsonschema_description:"The shaping set to add"`
}

type ShapeSetResponse struct {
	Type string `json:"type" jsonschema_description:"The type id of the response"`
	Set  string `json:"set" jsonschema_description:"The id of the set added"`
}

// ShapeClearRequest removes the shaping set named by Set, or every set when Set is
// empty.
type ShapeClearRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance to remove the sets from"`
	Set        string `json:"set,omitempty" jsonschema_description:"The id of the set to remove, every set when empty"`
}

type ShapeClearResponse struct {
	Type    string   `json:"type" jsonschema_description:"The type id of the response"`
	Cleared []string `json:"cleared" jsonschema_description:"The ids of the sets removed"`
}

// ShapeReportRequest reports the firings of the shaping set named by Set, or of
// every set when Set is empty.
type ShapeReportRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance to report on"`
	Set        string `json:"set,omitempty" jsonschema_description:"The id of the set to report, every set when empty"`
}

type ShapeReportResponse struct {
	Type string          `json:"type" jsonschema_description:"The type id of the response"`
	Sets []ShapingReport `json:"sets" jsonschema_description:"The firings of each set reported"`
}

// ShapingReport lists every time a rule of one set fired, on every server the set
// applies on, in the order of their time. Servers counts the firings by the name
// of the server whose capture proxy they fired on, with an entry, zero included,
// for every server the set applies on.
type ShapingReport struct {
	ID      string          `json:"id" jsonschema_description:"The set's id"`
	Firings []ShapingFiring `json:"firings" jsonschema_description:"Every firing of a rule of the set, in the order of their time"`
	Servers map[string]int  `json:"servers,omitempty" jsonschema_description:"The firings counted by the server whose capture proxy they fired on"`
}

// ShapingFiring records one rule firing on one frame. FrameID is the frame's id in
// the capture, so an assertion can find the shaped event, and Action is the kind
// that was applied.
type ShapingFiring struct {
	ConnectionUUID string    `json:"connection_uuid" jsonschema_description:"The id of the connection"`
	ClientName     string    `json:"client_name" jsonschema_description:"The CONNECT name of the client"`
	FrameID        string    `json:"frame_id" jsonschema_description:"The frame's id in the capture"`
	Rule           string    `json:"rule" jsonschema_description:"The id of the rule that fired"`
	Action         string    `json:"action" jsonschema_description:"The kind of action applied"`
	Time           time.Time `json:"time" jsonschema_description:"When the rule fired"`
}

// ShapeListRequest lists the shaping set named by Set, or every set when Set is
// empty, as each was submitted to shape.set, so a set can be read, changed and
// set again.
type ShapeListRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance whose sets to list"`
	Set        string `json:"set,omitempty" jsonschema_description:"The id of the set to list, every set when empty"`
}

type ShapeListResponse struct {
	Type string       `json:"type" jsonschema_description:"The type id of the response"`
	Sets []ShapingSet `json:"sets" jsonschema_description:"Each set listed, as it was submitted to shape.set"`
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
	// ToxicLossy emulates packet loss as TCP delivers it, as stalls rather
	// than lost bytes. The stream is cut into 1460-byte segments, each lost
	// with chance loss, above 0 and below 1, which is required, and data that
	// arrives after the stream was idle for 50ms starts a segment of its own,
	// as a write after a pause goes in a packet of its own. A lost segment
	// stalls the stream for rto ms, 200 by default, and each retransmission
	// lost again doubles the stall, up to max_rto ms, 3200 by default. Later
	// data waits behind it, and no byte is lost or reordered while it is in
	// place; removing it while the stream after it accepts nothing for more
	// than 5s drops the data it held, as toxiproxy does when it removes a
	// toxic. Its attributes are the agent's own, not toxiproxy's. Its
	// toxicity is rolled once for each stream of each connection and kept
	// when the stream's chain is rebuilt.
	ToxicLossy = "lossy"
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
	Kind string `json:"kind,omitempty" jsonschema_description:"The kind of link, route, gateway or client, every kind when empty"`
	// From is a node or a cluster at one end of the links. Empty matches any.
	// A client link has no From.
	From string `json:"from,omitempty" jsonschema_description:"A node or a cluster at one end of the links, any when empty"`
	// To is a node or a cluster at the other end of the links, and the server
	// of a client link. Empty matches any.
	To string `json:"to,omitempty" jsonschema_description:"A node or a cluster at the other end of the links, or the server of a client link, any when empty"`
	// Node selects every link of one node, both ways. It cannot be combined
	// with From or To. A cut of a node alone unplugs it.
	Node string `json:"node,omitempty" jsonschema_description:"Selects every link of one node both ways, not combined with from or to"`
	// Direction is one of the ToxicDirection* constants. Empty means both.
	Direction string `json:"direction,omitempty" jsonschema_description:"The data the toxic acts on, to, from or both, both when empty"`
}

// Toxic is a named fault on the links a selector names. Type is one of the
// Toxic* constants. Attributes are those of toxiproxy's toxic of that type,
// by toxiproxy's names and in its units, or for ToxicLossy the agent's own,
// which the agent validates before it applies them. Toxicity is the chance,
// in [0, 1], that the toxic acts, 1 when nil: toxiproxy rolls it for each
// stream of each connection, and again whenever the stream's chain is rebuilt
// by adding or removing another toxic, while ToxicLossy rolls it once for
// each stream. A cut cuts every connection and accepts no toxicity other
// than 1.
type Toxic struct {
	Name       string         `json:"name" jsonschema_description:"The toxic's name, unique on the instance"`
	Selector   ToxicSelector  `json:"selector" jsonschema_description:"The links the toxic acts on"`
	Type       string         `json:"type" jsonschema_description:"The toxic's type"`
	Attributes map[string]any `json:"attributes,omitempty" jsonschema_description:"The attributes of the toxic's type"`
	Toxicity   *float64       `json:"toxicity,omitempty" jsonschema_description:"The chance in [0, 1] that the toxic acts, 1 when absent"`
}

// ActiveToxic is a toxic in place on an instance, with the names of the links
// its selector resolved to, in report order, and when it was added.
type ActiveToxic struct {
	Toxic
	Links []string  `json:"links" jsonschema_description:"The names of the links the selector resolved to"`
	Since time.Time `json:"since" jsonschema_description:"When the toxic was added"`
}

// LinkToxics is one proxied link of an instance and the toxics on it, in the
// order they were added.
type LinkToxics struct {
	Link
	Toxics []LinkToxic `json:"toxics,omitempty" jsonschema_description:"The toxics on the link, in the order they were added"`
}

// LinkToxic is one toxic on one link, as the caller sees it. Direction is the
// data it acts on, relative to the link: ToxicDirectionTo for the data
// flowing from the link's From toward its To, ToxicDirectionFrom for the
// reverse, and ToxicDirectionBoth for both. Node, set only on a gateway link,
// limits a cut to the connections of that source node, since the link's
// proxy is shared by every node of its cluster.
type LinkToxic struct {
	Name       string         `json:"name" jsonschema_description:"The toxic's name"`
	Type       string         `json:"type" jsonschema_description:"The toxic's type"`
	Direction  string         `json:"direction" jsonschema_description:"The data the toxic acts on relative to the link, to, from or both"`
	Node       string         `json:"node,omitempty" jsonschema_description:"The source node a cut on a gateway link is limited to"`
	Attributes map[string]any `json:"attributes,omitempty" jsonschema_description:"The attributes of the toxic's type"`
	Toxicity   float64        `json:"toxicity" jsonschema_description:"The chance in [0, 1] that the toxic acts"`
	Since      time.Time      `json:"since" jsonschema_description:"When the toxic was added"`
}

// ToxicAddRequest adds a toxic to an instance created with toxics. Its name
// must not be in use on the instance.
type ToxicAddRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance to add the toxic to"`
	Toxic      Toxic  `json:"toxic" jsonschema_description:"The toxic to add"`
}

type ToxicAddResponse struct {
	Type  string      `json:"type" jsonschema_description:"The type id of the response"`
	Toxic ActiveToxic `json:"toxic" jsonschema_description:"The toxic as applied"`
}

// ToxicRemoveRequest removes the toxic named Name from an instance.
type ToxicRemoveRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance to remove the toxic from"`
	Name       string `json:"name" jsonschema_description:"The name of the toxic to remove"`
}

type ToxicRemoveResponse struct {
	Type    string `json:"type" jsonschema_description:"The type id of the response"`
	Removed string `json:"removed" jsonschema_description:"The name of the toxic removed"`
}

// ToxicClearRequest removes every toxic from an instance.
type ToxicClearRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance to remove every toxic from"`
}

// ToxicClearResponse lists the names of the toxics removed, in the order they
// were added.
type ToxicClearResponse struct {
	Type    string   `json:"type" jsonschema_description:"The type id of the response"`
	Cleared []string `json:"cleared" jsonschema_description:"The names of the toxics removed, in the order they were added"`
}

// ToxicReportRequest reports the links of an instance and the toxics on them.
type ToxicReportRequest struct {
	Type       string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	InstanceID string `json:"instance_id" jsonschema:"required,minLength=1" jsonschema_description:"The instance to report on"`
}

// ToxicReportResponse lists every proxied link of an instance with the toxics
// on it, in the order the create response lists the links, and every toxic in
// place, in the order they were added.
type ToxicReportResponse struct {
	Type   string        `json:"type" jsonschema_description:"The type id of the response"`
	Links  []LinkToxics  `json:"links" jsonschema_description:"Every proxied link with the toxics on it"`
	Toxics []ActiveToxic `json:"toxics" jsonschema_description:"Every toxic in place, in the order they were added"`
}

// ClockRequest asks the service for the clocks of the participants it knows,
// the service itself and each instance's agent, and of each subject in Extra.
// Each extra subject is a participant's clock endpoint, such as a workload
// engine's, which the service probes.
type ClockRequest struct {
	Type  string   `json:"type,omitempty" jsonschema_description:"The type id of the request"`
	Extra []string `json:"extra,omitempty" jsonschema_description:"Clock subjects of further participants for the service to probe"`
}

// ClockResponse reports each participant's clock against the service host's
// clock, the service first, then the agents in instance id order, then the
// extra subjects in the order the request listed them. It holds no threshold
// and no verdict; the caller judges the offsets.
type ClockResponse struct {
	Type         string             `json:"type" jsonschema_description:"The type id of the response"`
	Participants []ClockParticipant `json:"participants" jsonschema_description:"Each participant's clock, the service first, then the agents, then the extra subjects"`
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
	Kind string `json:"kind" jsonschema_description:"The kind of participant, service, agent or extra"`

	// InstanceID is the instance an agent fronts.
	InstanceID string `json:"instance_id,omitempty" jsonschema_description:"The instance an agent fronts"`

	// Subject is the clock subject of an extra participant.
	Subject string `json:"subject,omitempty" jsonschema_description:"The clock subject of an extra participant"`

	// Reference is true for the service, whose host's clock every offset is
	// measured against.
	Reference bool `json:"reference,omitempty" jsonschema_description:"True for the service, whose host's clock every offset is measured against"`

	// SharedClock is true for a participant that runs in the service's
	// process and so reads the service host's clock. It is reported with a
	// zero offset without being probed.
	SharedClock bool `json:"shared_clock,omitempty" jsonschema_description:"True for a participant that reads the service host's clock, reported with a zero offset without a probe"`

	Offset    time.Duration `json:"offset" jsonschema_description:"The participant's clock less the service host's in nanoseconds, positive when it is ahead"`
	RoundTrip time.Duration `json:"round_trip" jsonschema_description:"The probe's time in flight in nanoseconds"`
	Bound     time.Duration `json:"bound" jsonschema_description:"Half the round trip in nanoseconds, the true offset lies within it of offset"`

	// Error is why an extra participant could not be measured, such as no
	// reply. Offset, RoundTrip and Bound are zero when it is set.
	Error string `json:"error,omitempty" jsonschema_description:"Why an extra participant could not be measured, offset, round_trip and bound are zero when set"`
}

// SchemaRequest is the request of the schema endpoint, which reads nothing
// from it.
type SchemaRequest struct {
	Type string `json:"type,omitempty" jsonschema_description:"The type id of the request"`
}

// SchemaResponse holds the JSON Schemas of the request and response of every
// endpoint.
type SchemaResponse struct {
	Type      string           `json:"type" jsonschema_description:"The type id of the response"`
	Endpoints []EndpointSchema `json:"endpoints" jsonschema_description:"The schemas of every endpoint"`
}

// EndpointSchema holds the request and response schemas of one endpoint. Name
// is the endpoint's name below the service group, such as "create.server".
type EndpointSchema struct {
	Name        string          `json:"name" jsonschema_description:"The endpoint's name below the service group, such as create.server"`
	Description string          `json:"description" jsonschema_description:"What the endpoint does"`
	Request     json.RawMessage `json:"request" jsonschema:"type=object" jsonschema_description:"The JSON Schema of the endpoint's request"`
	Response    json.RawMessage `json:"response" jsonschema:"type=object" jsonschema_description:"The JSON Schema of the endpoint's response"`
}
