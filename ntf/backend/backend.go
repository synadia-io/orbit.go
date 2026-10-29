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

package backend

import (
	"context"
	"log/slog"
)

// Capability is something a backend can do beyond what every backend must. A
// backend declares its capabilities through Backend.Capabilities, and refuses
// the calls that need one it does not declare with ErrUnsupported.
type Capability string

const (
	// CapabilityKill means Node.Kill stops a server abruptly.
	CapabilityKill Capability = "kill"

	// CapabilityPause means Node.Pause and Node.Resume freeze and continue a
	// server.
	CapabilityPause Capability = "pause"

	// CapabilityVersionChange means Node.Apply accepts a revision with a
	// different Version.
	CapabilityVersionChange Capability = "version-change"
)

// Options is what the service passes a Factory.
type Options struct {
	// Dir is the service's directory, which holds per-instance state.
	Dir string

	// Preserve keeps instance directories on Backend.Teardown, for debugging.
	Preserve bool

	// Logger receives the backend's log lines.
	Logger *slog.Logger
}

// Factory builds a backend. ctx limits startup only: the backend runs until
// Backend.Close. Settings that only one backend needs, such as a Docker host or
// a kubeconfig, reach it by closing over them in its factory.
type Factory func(ctx context.Context, opts Options) (Backend, error)

// Backend runs the servers of whole instances. Its name is the name its
// Factory is registered under.
//
// For each instance the service calls Placement, plans the instance against
// it, then calls Provision with the plan. It calls Teardown to remove the
// instance, at any point after Placement returned: after a failed plan, after
// a failed Provision, while Provision is still starting the instance, or when
// the instance is destroyed.
//
// The service calls a backend's methods concurrently, for different instances
// and, through Teardown, for the instance a Provision is starting.
// Capabilities and DefaultVersion are safe to call at any time.
type Backend interface {
	// Capabilities returns the capabilities the backend declares.
	Capabilities() []Capability

	// DefaultVersion returns the server version the service fills into every
	// NodePlan.Version it plans, in the backend's own version string.
	DefaultVersion() string

	// Placement returns the placement of the instance instanceID: the
	// endpoint of each listener of each node and the directories of the
	// instance. Once it returns, Teardown removes whatever it reserved.
	Placement(ctx context.Context, instanceID string) (Placement, error)

	// Provision writes the files of plan, its instance Files and each node's
	// Files and Config, then starts every node in plan order, and returns one
	// Node for each, in plan order.
	//
	// It is all or nothing: on any failure it stops the nodes it started and
	// returns the error. A failure of a server to bind one of its ports wraps
	// ErrListen, and the service then plans the instance again with a fresh
	// placement. When Teardown of the same instance runs while Provision is
	// starting it, Provision starts no further node, stops the ones it
	// started and fails.
	//
	// It must not keep plan, or any of its file contents, once it returns:
	// the files may hold private keys. A file path is relative to the
	// instance directory, and a node's config goes in the node's directory,
	// since it includes the node's files by paths relative to that directory.
	Provision(ctx context.Context, plan *InstancePlan) ([]Node, error)

	// Teardown removes everything the backend holds for the instance
	// instanceID: its started nodes, its reserved ports and its directories,
	// keeping the directories when Options.Preserve is set. It is valid at any
	// point after Placement returned for the instance, and calling it again
	// does nothing. Once it returns, every method of the instance's nodes that
	// returns an error fails.
	Teardown(ctx context.Context, instanceID string) error

	// Close releases what the backend holds for the whole service, such as a
	// client of a container runtime. The service calls it once, after tearing
	// down every instance, and calls nothing on the backend after it.
	Close() error
}

// Placement is where one instance runs, as the service plans it: the endpoint
// of each listener and the directories its files go in. Backend.Placement
// returns it.
//
// Every path is a path as the server sees it, which is what the server's
// config renders. A backend that runs servers somewhere other than its own
// host maps these paths to where it writes.
type Placement interface {
	// ListenerAddress returns the listener of the given name of the node named
	// node, NodePlan.Name, with the endpoint the node's config renders for it.
	// The returned Listener's Name is listener. The service asks for each
	// listener of each node once. A failure, such as no free port, fails the
	// plan.
	ListenerAddress(node, listener string) (Listener, error)

	// ProxyAddress returns the endpoint of the proxy named name, which the
	// instance's network agent runs. kind is "route" or "gateway" for a proxy
	// between nodes, which every node must reach, and "client" for a proxy in
	// front of a node's client listener, which clients reach; the nodes'
	// configs render these three. The service asks for each of them once,
	// after every listener, and only for an instance created with toxics.
	//
	// Kind "trace", with name "trace:<node>", asks for the trace port of a
	// node: the capture proxy clients reach instead of the node, or instead
	// of its client proxy under toxics. No config renders it. The service
	// asks for one per node, after every other address, and only for an
	// instance created with trace or proxy, with or without toxics.
	//
	// Kind "advertise", with name "advertise", asks instead for the
	// instance's dead advertise address, which its nodes advertise for their
	// routes and gateways. Nothing may listen on it, so a dial to it is
	// refused, and it is kept from any other use until Teardown. The service
	// asks for it once, for a cluster or super-cluster created with toxics.
	//
	// A failure, such as no free port, fails the plan.
	ProxyAddress(name, kind string) (Endpoint, error)

	// InstanceDir returns the instance directory. Every NodePlan.Dir and every
	// File.Path is relative to it.
	InstanceDir() string

	// StoreDir returns the store directory of the node named node,
	// NodePlan.Name. The node's config renders it as its store directory, and
	// it is one of the node's NodePlan.Persistent paths.
	StoreDir(node string) string
}

// Node is one server of an instance, as Backend.Provision returns it.
//
// Name, Addresses and Status are safe to call at any time, concurrently with
// any other call. The caller serializes the lifecycle calls (Start, Stop and
// Reload), Apply and the fault calls (Kill, Pause and Resume) on one node.
//
// Once Backend.Teardown of the node's instance returns, every method that
// returns an error fails.
type Node interface {
	// Name returns the node's NodePlan.Name. It identifies the node and never
	// changes.
	Name() string

	// Addresses returns, for each of the node's listeners, the endpoint a
	// proxy beside the node dials and the endpoint clients dial. They are
	// fixed when Backend.Provision returns and stay valid while the node is
	// stopped.
	Addresses() []Address

	// Status reads the node's state from where it runs. The server's name and
	// cluster are reported while the node is stopped as well. The cluster
	// port is read live, so a stopped node reports none.
	Status(ctx context.Context) (NodeStatus, error)

	// Start starts the stopped node from its current config, on the same
	// listeners and the same state.
	Start(ctx context.Context) error

	// Stop shuts the running node down gracefully. Its listeners, config and
	// state are kept for Start.
	Stop(ctx context.Context) error

	// Reload makes the running server read its config again.
	Reload(ctx context.Context) error

	// Apply takes the node's next plan revision. It works on a stopped node
	// and never reloads. Of the revision's Files, each is written; a file the
	// revision does not carry is kept. The revision's Config replaces the
	// node's config. The rule:
	//
	//   - Only Config or Files changed: write them.
	//   - Version changed: stop the node gracefully, swap its binary or image,
	//     and start it on the same state. A backend that does not declare
	//     CapabilityVersionChange refuses with ErrUnsupported.
	//   - Name, Dir, Cluster, Listeners or Persistent changed: refuse, naming
	//     what changed.
	//
	// It must not keep plan, or any of its file contents, once it returns.
	Apply(ctx context.Context, plan *NodePlan) error

	// Kill stops the node abruptly, with no graceful shutdown, as a kill -9
	// does. A backend that does not declare CapabilityKill refuses with
	// ErrUnsupported.
	Kill(ctx context.Context) error

	// Pause freezes the running node until Resume. A backend that does not
	// declare CapabilityPause refuses with ErrUnsupported.
	Pause(ctx context.Context) error

	// Resume continues a node Pause froze. A backend that does not declare
	// CapabilityPause refuses with ErrUnsupported.
	Resume(ctx context.Context) error
}

// Address is where one listener of a node is reached.
type Address struct {
	// Name is the listener's name, a Listener.Name of the node's plan.
	Name string

	// Internal is the endpoint a proxy beside the node dials.
	Internal Endpoint

	// External is the endpoint clients dial.
	External Endpoint
}

// NodeStatus is a node's state as Node.Status reads it from where the node
// runs.
type NodeStatus struct {
	// ServerName is the server's own name, which a custom template can set,
	// though an instance created with toxics must keep it as the node's name,
	// NodePlan.Name. It is reported while the node is stopped as well.
	ServerName string

	// Cluster is the name of the server's cluster, empty for none. It is
	// reported while the node is stopped as well.
	Cluster string

	// Running reports whether the server is running.
	Running bool

	// ClusterPort is the port the server's cluster listener is bound to, 0
	// when it has none or is stopped.
	ClusterPort int

	// Version is the server version the node runs, in the form of
	// NodePlan.Version.
	Version string
}
