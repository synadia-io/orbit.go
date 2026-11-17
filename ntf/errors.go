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

package ntf

import (
	"errors"

	"github.com/synadia-io/orbit.go/ntf/backend"
)

var (
	// ErrInvalidPortRange means Options.PortRange is not a usable range: both ends
	// must lie in 1-65535 and Low must be below High.
	ErrInvalidPortRange = errors.New("invalid port range")

	// ErrPortRangeOverlapsEphemeral means the managed server port range reaches into
	// the range the OS draws ports for outgoing connections from, where any process
	// on the host can take a port between the service reserving it and a managed
	// server binding it.
	ErrPortRangeOverlapsEphemeral = errors.New("port range overlaps the OS ephemeral port range")

	// ErrPortRangeExhausted means every port in the range is either handed out to a
	// managed server or held by another program.
	ErrPortRangeExhausted = errors.New("no free port in the port range")

	// ErrEmbeddedPortExhausted means no port the OS offered the embedded server was
	// clear on the loopback addresses.
	ErrEmbeddedPortExhausted = errors.New("no free port for the embedded server")

	// ErrServerFatal means a managed server or the embedded server reported a
	// fatal error.
	ErrServerFatal = errors.New("server reported a fatal error")

	// ErrServerListen means a managed server could not bind one of its ports,
	// usually because another program took it after the service reserved it, or
	// the embedded server could not bind its port.
	ErrServerListen = backend.ErrListen

	// ErrBackendName means a backend in Options.Backends cannot be registered
	// under its name: the name is empty or already registered.
	ErrBackendName = errors.New("invalid backend name")

	// ErrUnknownBackend means Options.Backend names no registered backend.
	ErrUnknownBackend = errors.New("unknown backend")

	// ErrAgentName means an agent in Options.Agents cannot be registered under
	// its name: the name is empty.
	ErrAgentName = errors.New("invalid agent name")

	// ErrUnknownAgent means Options.Agent names no registered agent.
	ErrUnknownAgent = errors.New("unknown agent")

	// ErrEventPublish means an event could not be published on the
	// management connection.
	ErrEventPublish = errors.New("could not publish event")
)

// errRouteTLS is the cause of a create or update with toxics refused because
// a node's config enables TLS on its routes or gateways, whose proxies read a
// plain CONNECT.
var errRouteTLS = errors.New("toxics needs plaintext routes and gateways")

// errGatewayName is the cause of a create or update of a super-cluster with
// toxics refused because a node's gateway is not named for its cluster, the
// name the gateway gate admits a connection by.
var errGatewayName = errors.New("toxics needs each gateway named for its cluster")

// errServerName is the cause of a create or update with toxics refused
// because a node does not set server_name to its node name, the name toxic
// selectors and the agent's gateway cut know the node by.
var errServerName = errors.New("toxics requires each node to set server_name to its node name")

// errConfigParse is the cause of a create or update with toxics refused
// because a node's config does not parse, so the checks of its routes and
// gateways cannot read it.
var errConfigParse = errors.New("toxics needs a config that parses")

// errServerNotFound is the cause of a stop, start, update or reload of a
// server refused because no server reports the name, in the instance the
// request names or, without one, in any instance.
var errServerNotFound = errors.New("server not found")

// errServerNameAmbiguous is the cause of a stop, start, update or reload of a
// server refused because more than one server reports the name, in one
// instance or across several. Clients that know the instance retry with its
// instance_id on this refusal.
var errServerNameAmbiguous = errors.New("server name is ambiguous")

// errJetStreamNotReady is the cause of a create with wait_jetstream answered
// with 024 because JetStream was not ready on every node when the wait ended.
var errJetStreamNotReady = errors.New("JetStream not ready")

// errInstanceDestroyed is the cause of a create with wait_jetstream answered
// with 404 because its instance was destroyed while it waited.
var errInstanceDestroyed = errors.New("instance destroyed")
