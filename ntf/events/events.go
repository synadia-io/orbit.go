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

import (
	"context"
	"time"
)

// Type ids of the events this package defines. A breaking change to a type is
// a new v2 id; an additive field is not.
const (
	TypeInstanceAction = "io.synadia.ntf.event.v1.instance_action"
	TypeNodeAction     = "io.synadia.ntf.event.v1.node_action"
	TypeShapingAction  = "io.synadia.ntf.event.v1.shaping_action"
	TypeShapingFired   = "io.synadia.ntf.event.v1.shaping_fired"
	TypeCaptureStored  = "io.synadia.ntf.event.v1.capture_stored"
	TypeToxicAction    = "io.synadia.ntf.event.v1.toxic_action"
	TypeChaosRun       = "io.synadia.ntf.event.v1.chaos_run"
	TypeChaosAction    = "io.synadia.ntf.event.v1.chaos_action"
	TypeChaosWindow    = "io.synadia.ntf.event.v1.chaos_window"
)

// Source names the component that published an event.
type Source string

const (
	// SourceService is the ntf service.
	SourceService Source = "service"
	// SourceAgent is a network agent fronting an instance's servers.
	SourceAgent Source = "agent"
	// SourceBackend is reserved for a backend running an instance's servers.
	SourceBackend Source = "backend"
	// SourceChaos is a chaos engine.
	SourceChaos Source = "chaos"
)

// Category is the group of kinds an event belongs to, the second to last
// token of its subject.
type Category string

const (
	CategoryInstance Category = "instance"
	CategoryNode     Category = "node"
	CategoryShaping  Category = "shaping"
	CategoryCapture  Category = "capture"
	CategoryToxic    Category = "toxic"
	CategoryChaos    Category = "chaos"
)

// Base holds the fields every event carries. Every event struct embeds it.
type Base struct {
	// Type is the event's type id, such as TypeNodeAction.
	Type string `json:"type"`

	// ID is a nuid unique to this event.
	ID string `json:"id"`

	// Timestamp is the publisher's clock when the event was made. It is
	// encoded in RFC 3339 with nanoseconds.
	Timestamp time.Time `json:"timestamp"`

	// InstanceID is the instance the event is about.
	InstanceID string `json:"instance_id"`

	// Source is the component that published the event.
	Source Source `json:"source"`

	// SourceSeq counts this source's events for this instance from 1 with no
	// gaps.
	SourceSeq uint64 `json:"source_seq"`
}

// EventBase returns the common fields, for a publisher to stamp and a reader
// to read.
func (b *Base) EventBase() *Base {
	return b
}

// Event is any event. A pointer to an event struct implements it.
type Event interface {
	// EventBase returns the event's common fields.
	EventBase() *Base

	// EventType returns the event's type id. For a known type this is the
	// type's constant, whatever Base.Type holds, so a publisher can stamp it.
	EventType() string

	// EventCategory returns the event's category, which its subject carries.
	EventCategory() Category

	// EventAction returns the event's action, which its subject carries.
	EventAction() string
}

// Publisher publishes events. The service implements it and hands it to the
// agent, which publishes its own events through it. The caller fills
// instance_id and the fields of the event's type; the publisher stamps type,
// id, timestamp in UTC, source and source_seq.
type Publisher interface {
	// Publish publishes e on the subject Subject builds for it.
	Publish(ctx context.Context, e Event) error
}
