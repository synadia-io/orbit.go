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

package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/synadia-io/orbit.go/ntf/backend"
)

// Kind is a named factory of actions: a disruptive thing a worker does, such
// as a stream leader stepdown or a toxic on some links. It is registered by
// name in a kind Registry, and a worker's action names it in the keyed form.
//
// Info and New are safe to call concurrently.
type Kind interface {
	// Info describes the kind: its params schema and whether it holds.
	Info() KindInfo

	// New returns an action for the params of one worker's action, the value
	// under the kind's name in the keyed form. The params have been checked
	// against the kind's schema; New refuses params it still cannot use. The
	// engine calls New once for each worker that names the kind, for every
	// worker when a run starts, and checks each action's Needs then.
	New(params json.RawMessage) (Action, error)
}

// KindInfo describes a kind. Fields may be added to it, so a kind returns it
// as a struct literal with named fields. What a kind needs of the instance and
// the run depends on its params, so each Action reports it, through
// Action.Needs.
type KindInfo struct {
	// Schema is the JSON Schema of the kind's params.
	Schema json.RawMessage

	// Holds reports that the kind's Apply returns an Effect to hold and
	// revert, as a toxic does. A kind that does not hold is instant, such as
	// a stepdown, whose Apply returns no Effect unless it was cut short.
	Holds bool
}

// Needs is what one action needs of the instance it acts on and of the run,
// given its params. When a run starts, the engine checks the needs of every
// worker's action: that the instance's backend declares each of
// Capabilities, that the instance was created with toxics when Toxics is set,
// and that each account in Accounts has a credential in the definition or a
// user of the default template. It refuses the run, naming what is missing,
// when one of these fails. The account check is by name only: a credential
// that is wrong, or that belongs to another account, shows up as a failed
// Apply, not when the run starts.
type Needs struct {
	// Capabilities are the backend capabilities the action needs the
	// instance's backend to declare.
	Capabilities []backend.Capability

	// Toxics means the action needs the instance created with toxics, on a
	// network agent that declares the toxics capability.
	Toxics bool

	// Accounts are the names of the accounts the action needs credentials
	// for, such as the account of the streams its params name, and
	// SystemAccount when it uses the system account, such as to wait for the
	// servers to be healthy.
	Accounts []string
}

// Action is one worker's instance of a kind, made by Kind.New with the
// worker's params. A worker calls Apply each time its driver says to act,
// never concurrently.
type Action interface {
	// Needs returns what the action needs of the instance and the run, given
	// the params it was made with. It returns the same on every call. The
	// engine checks it when the run starts, as Needs says.
	Needs() Needs

	// Apply acts on target once and returns the Effect to revert later, or
	// nil for an instant action. It reports the targets it acts on, and the
	// steps of a long Apply, through steps.
	//
	// Apply honors ctx: when ctx ends it stops, and returns an Effect that
	// reverts the work it did so far, or nil when there is none, along with
	// an error. The engine holds a non-nil Effect whatever the error.
	Apply(ctx context.Context, target Target, steps StepReporter) (Effect, error)
}

// Effect is the lasting part of an Apply, held open until the worker reverts
// it. Each Effect has one owner, which calls Revert, again after a failure,
// until it succeeds.
type Effect interface {
	// Revert undoes the effect. It runs on a context of its own, not the one
	// the Apply had. A Revert that finds its target already in the wanted
	// state, such as a node already running or a toxic already gone,
	// succeeds.
	Revert(ctx context.Context) error
}

// StepReporter is how an Apply tells the engine what it acts on and how it
// progresses. The engine makes one for each Apply and publishes what it is
// told on the events of that action. It is safe to call concurrently.
type StepReporter interface {
	// Acted records targets the Apply acted on, which the engine names on
	// the action's applied or apply_failed event. Each call adds to what the
	// earlier ones recorded.
	Acted(targets Targets)

	// Step reports one step of a long Apply, such as a rolling restart
	// stopping one node, which the engine publishes as a step of the action.
	Step(step Step)
}

// Step is one step of a long Apply.
type Step struct {
	// Description says what happened, such as "stopped n1".
	Description string `json:"description"`

	// Targets are what the step acted on.
	Targets Targets `json:"targets,omitzero"`
}

// Targets names what an Apply or one of its steps acted on.
type Targets struct {
	// Nodes are server names.
	Nodes []string `json:"nodes,omitempty"`

	// Toxics are toxic names.
	Toxics []string `json:"toxics,omitempty"`

	// Streams are streams by account and name.
	Streams []StreamRef `json:"streams,omitempty"`

	// Consumers are consumers by account, stream and name.
	Consumers []ConsumerRef `json:"consumers,omitempty"`

	// Clients are client connections by server and client id, since a client
	// id is unique on its server only.
	Clients []ClientRef `json:"clients,omitempty"`
}

// StreamRef names one stream.
type StreamRef struct {
	Account string `json:"account"`
	Name    string `json:"name"`
}

// ConsumerRef names one consumer.
type ConsumerRef struct {
	Account string `json:"account"`
	Stream  string `json:"stream"`
	Name    string `json:"name"`
}

// ClientRef names one client connection.
type ClientRef struct {
	// Server is the name of the server the client is connected to.
	Server string `json:"server"`

	// ID is the client id the server gave the connection.
	ID uint64 `json:"id"`
}

// DecodeParams decodes the params of a driver or kind into v, refusing fields
// v does not have, field names that differ from v's only in case, and data
// after the value. Empty params decode as an empty object.
func DecodeParams(params json.RawMessage, v any) error {
	if len(bytes.TrimSpace(params)) == 0 {
		params = json.RawMessage("{}")
	}

	err := decodeStrict(params, v)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidParams, err)
	}

	return nil
}
