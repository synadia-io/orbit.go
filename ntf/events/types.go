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

package events

import "time"

// InstanceEventAction is what happened to an instance.
type InstanceEventAction string

const (
	InstanceCreated   InstanceEventAction = "created"
	InstanceDestroyed InstanceEventAction = "destroyed"
)

// DestroyReason is the request that destroyed an instance.
type DestroyReason string

const (
	ReasonDestroy DestroyReason = "destroy"
	ReasonReset   DestroyReason = "reset"
	ReasonClose   DestroyReason = "close"
)

// InstanceAction is published when an instance is created or destroyed.
//
// NTF Schema Type io.synadia.ntf.event.v1.instance_action
type InstanceAction struct {
	Base

	Action InstanceEventAction `json:"action"`

	// Kind is the instance's kind, such as server, cluster or super-cluster.
	// Set on created.
	Kind string `json:"kind,omitempty"`

	// Backend is the name of the backend running the instance's servers. Set
	// on created.
	Backend string `json:"backend,omitempty"`

	// Reason is the request that destroyed the instance. Set on destroyed.
	Reason DestroyReason `json:"reason,omitempty"`
}

func (e *InstanceAction) EventType() string       { return TypeInstanceAction }
func (e *InstanceAction) EventCategory() Category { return CategoryInstance }
func (e *InstanceAction) EventAction() string     { return string(e.Action) }

// NodeEventAction is what happened to a node. NodeKilled, NodePaused and
// NodeResumed are defined but not published until a backend supports them.
type NodeEventAction string

const (
	NodeReady    NodeEventAction = "ready"
	NodeStopped  NodeEventAction = "stopped"
	NodeStarted  NodeEventAction = "started"
	NodeUpdated  NodeEventAction = "updated"
	NodeReloaded NodeEventAction = "reloaded"
	NodeKilled   NodeEventAction = "killed"
	NodePaused   NodeEventAction = "paused"
	NodeResumed  NodeEventAction = "resumed"
)

// NodeAction is published when a node of an instance changes state.
//
// NTF Schema Type io.synadia.ntf.event.v1.node_action
type NodeAction struct {
	Base

	Action NodeEventAction `json:"action"`

	// Node is the node's name in the create response.
	Node string `json:"node"`

	// ServerName is the name status shows and stop.server takes.
	ServerName string `json:"server_name"`
}

func (e *NodeAction) EventType() string       { return TypeNodeAction }
func (e *NodeAction) EventCategory() Category { return CategoryNode }
func (e *NodeAction) EventAction() string     { return string(e.Action) }

// ShapingEventAction is what happened to a shaping set.
type ShapingEventAction string

const (
	ShapingApplied ShapingEventAction = "applied"
	ShapingCleared ShapingEventAction = "cleared"
)

// ShapingAction is published when a shaping set is applied to or cleared from
// an instance's capture proxy, one event per set. Clearing every set publishes
// one cleared event per set removed.
//
// NTF Schema Type io.synadia.ntf.event.v1.shaping_action
type ShapingAction struct {
	Base

	Action ShapingEventAction `json:"action"`

	// Set is the shaping set's id.
	Set string `json:"set"`
}

func (e *ShapingAction) EventType() string       { return TypeShapingAction }
func (e *ShapingAction) EventCategory() Category { return CategoryShaping }
func (e *ShapingAction) EventAction() string     { return string(e.Action) }

// ShapingFiredAction is the action of every ShapingFired event.
const ShapingFiredAction = "fired"

// ShapingFired is published when a shaping rule fires on a frame.
//
// NTF Schema Type io.synadia.ntf.event.v1.shaping_fired
type ShapingFired struct {
	Base

	// Set is the id of the shaping set holding the rule.
	Set string `json:"set"`

	// Rule is the id of the rule that fired.
	Rule string `json:"rule"`

	// RuleAction is what the rule did to the frame: drop, stall, throttle or
	// disconnect.
	RuleAction string `json:"rule_action"`

	// Wait is how long a stall or throttle held the frame, zero otherwise. It
	// is encoded in nanoseconds.
	Wait time.Duration `json:"wait"`

	// ConnectionUUID is the connection's uuid, the capture header's cuuid and
	// the connection_uuid in TRACES metadata.
	ConnectionUUID string `json:"connection_uuid"`

	// ClientName is the connection's CONNECT name.
	ClientName string `json:"client_name"`

	// FrameID is the id of the frame the rule acted on in the capture.
	FrameID string `json:"frame_id"`
}

func (e *ShapingFired) EventType() string       { return TypeShapingFired }
func (e *ShapingFired) EventCategory() Category { return CategoryShaping }
func (e *ShapingFired) EventAction() string     { return ShapingFiredAction }

// CaptureStoredAction is the action of every CaptureStored event.
const CaptureStoredAction = "stored"

// CaptureStored is published when a connection's capture is stored.
//
// NTF Schema Type io.synadia.ntf.event.v1.capture_stored
type CaptureStored struct {
	Base

	// Bucket is the object store holding the capture.
	Bucket string `json:"bucket"`

	// Object is the capture's object name in Bucket.
	Object string `json:"object"`

	// ConnectionUUID is the connection's uuid, the capture header's cuuid and
	// the connection_uuid in TRACES metadata.
	ConnectionUUID string `json:"connection_uuid"`

	// ClientName is the connection's CONNECT name.
	ClientName string `json:"client_name"`

	// ServerName is the name of the server the connection reached.
	ServerName string `json:"server_name"`

	// Shaped reports whether any shaping rule fired on the connection.
	Shaped bool `json:"shaped"`

	// Truncated reports whether the capture was cut short, such as by an
	// abrupt close.
	Truncated bool `json:"truncated"`
}

func (e *CaptureStored) EventType() string       { return TypeCaptureStored }
func (e *CaptureStored) EventCategory() Category { return CategoryCapture }
func (e *CaptureStored) EventAction() string     { return CaptureStoredAction }

// ToxicEventAction is what happened to a toxic.
type ToxicEventAction string

const (
	ToxicAdded   ToxicEventAction = "added"
	ToxicRemoved ToxicEventAction = "removed"
	ToxicCleared ToxicEventAction = "cleared"
)

// ToxicSelector names the links of an instance a toxic acts on. It has the
// fields and JSON names of the management API's toxic selector, copied so
// this package imports nothing from ntf.
type ToxicSelector struct {
	// Kind is route, gateway or client. Empty selects every kind.
	Kind string `json:"kind,omitempty"`

	// From is a node or a cluster at one end of the links. Empty matches any.
	From string `json:"from,omitempty"`

	// To is a node or a cluster at the other end of the links, and the server
	// of a client link. Empty matches any.
	To string `json:"to,omitempty"`

	// Node selects every link of one node, both ways.
	Node string `json:"node,omitempty"`

	// Direction is to, from or both.
	Direction string `json:"direction,omitempty"`
}

// ToxicAction is published when a toxic is added to or removed from the
// links of an instance, one event per toxic. Clearing every toxic publishes
// one cleared event per toxic removed. Every action carries the toxic as it
// was applied, so a reader needs no earlier event.
//
// NTF Schema Type io.synadia.ntf.event.v1.toxic_action
type ToxicAction struct {
	Base

	Action ToxicEventAction `json:"action"`

	// Name is the toxic's name, unique on the instance.
	Name string `json:"name"`

	// Selector names the links the toxic acts on, with its direction filled
	// in.
	Selector ToxicSelector `json:"selector"`

	// ToxicType is the toxic's type, such as cut, latency or lossy.
	ToxicType string `json:"toxic_type"`

	// Attributes are the toxic's attributes as applied, with defaults
	// filled in. Absent when there are none, such as for a cut.
	Attributes map[string]any `json:"attributes,omitempty"`

	// Toxicity is the chance, in [0, 1], that the toxic acts.
	Toxicity float64 `json:"toxicity"`

	// Links are the names of the links the selector resolved to.
	Links []string `json:"links"`
}

func (e *ToxicAction) EventType() string       { return TypeToxicAction }
func (e *ToxicAction) EventCategory() Category { return CategoryToxic }
func (e *ToxicAction) EventAction() string     { return string(e.Action) }
