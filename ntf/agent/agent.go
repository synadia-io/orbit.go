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
)

// Options is what the service passes a Factory.
//
// Conn and Server are local resources for an agent running in the service's
// process. A remote agent ignores them.
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
}

// Factory builds a provider. ctx limits startup only: the provider runs until
// Provider.Close.
type Factory func(ctx context.Context, opts Options) (Provider, error)

// Provider starts the agents of a service, one for each instance that needs
// one. The service calls Start concurrently, for different instances.
type Provider interface {
	// Start starts an agent for the instance spec describes and returns it.
	Start(ctx context.Context, spec Spec) (Agent, error)

	// Close releases what the provider holds for the whole service. The
	// service calls it once, after every instance is gone and before the
	// management server stops.
	Close() error
}

// Spec describes the managed server an agent is to front.
type Spec struct {
	// InstanceID is the instance the server belongs to.
	InstanceID string

	// ServerName is the managed server's name, recorded in captures.
	ServerName string

	// Client is the endpoint of the node's internal client address, which
	// the agent dials.
	Client backend.Endpoint

	// ListenHost is the host the agent binds and advertises. Empty binds all
	// interfaces.
	ListenHost string

	// Store is true when the agent keeps captures, and false when it only
	// forwards and shapes.
	Store bool
}

// Agent is a running network agent fronting one managed server. Every method
// takes and returns plain values, so it can become a NATS request and reply.
//
// The service calls an agent's methods concurrently, including while Stop runs
// and after it returns.
//
// An agent without shaping refuses Shape, ClearShaping, ShapingReport and
// ShapingSets with ErrUnsupported.
type Agent interface {
	// Endpoint returns where clients dial instead of the managed server. It
	// is fixed once Provider.Start returns.
	Endpoint() backend.Endpoint

	// Shape adds a set, replacing one with the same id and resetting its
	// counters. Its error names the rule and field it could not apply, and
	// reaches the caller as is.
	Shape(ctx context.Context, set api.ShapingSet) error

	// ClearShaping removes the set id, or every set when id is empty, and
	// returns the ids removed.
	ClearShaping(ctx context.Context, id string) ([]string, error)

	// ShapingReport returns the firings of the set id, or of every set when
	// id is empty.
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
