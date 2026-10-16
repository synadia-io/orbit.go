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
}

type ManagedServer struct {
	Name    string         `json:"name"`
	Cluster string         `json:"cluster"`
	Port    int            `json:"port"`
	Ports   map[string]int `json:"ports,omitempty"`
	URL     string         `json:"url,omitempty"`
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
