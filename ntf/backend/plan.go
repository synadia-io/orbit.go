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
	"io/fs"
	"net"
	"strconv"
)

// Endpoint is a host and port something is reached on.
type Endpoint struct {
	// Host is a host name or IP address.
	Host string

	// Port is the TCP port.
	Port int
}

// String returns the endpoint as host:port, with an IPv6 host in brackets.
func (e Endpoint) String() string {
	return net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
}

// InstancePlan is the record of what an instance is: its nodes, how they
// connect and every file they need, with every config and file already
// rendered. The service builds it against the instance's Placement and hands it
// to Backend.Provision. A backend must not keep it, or any of its file
// contents, once Provision returns: the files may hold private keys.
type InstancePlan struct {
	// ID is the instance id, the one Backend.Placement and Backend.Teardown
	// are called with.
	ID string

	// ShortID is the short, name-safe form of ID that prefixes node and
	// cluster names.
	ShortID string

	// Kind is the kind of instance: "server", "cluster" or "super-cluster".
	Kind string

	// Description is the caller's free-form description of the instance.
	Description string

	// Clusters names the instance's clusters in order. Empty for a single
	// server.
	Clusters []string

	// Nodes are the instance's nodes in start order. Backend.Provision starts
	// them in this order and returns one Node for each, in the same order.
	Nodes []*NodePlan

	// Links are the connections the nodes make to each other. They are already
	// part of each node's rendered config; a backend reads them to know which
	// node dials which.
	Links []Link

	// Files are the files the instance needs outside any one node, such as the
	// generated TLS material. Their paths do not start with any node's Dir.
	Files []File

	// ProxyNode names the first of the nodes whose client listeners the
	// service fronts with capture proxies, every node of an instance created
	// with trace or proxy. Its trace port is the instance's first trace URL.
	// Empty when the instance has none.
	ProxyNode string
}

// NodePlan is the plan for one node of an instance: what it listens on, the
// files it needs and the config it runs. A node's plan never changes once the
// node is provisioned; Node.Apply takes a new revision of it.
type NodePlan struct {
	// Name is the plan's identity for the node. It identifies the node and
	// never changes. It is not necessarily the server's own name, which a
	// custom template can set, though an instance created with toxics must
	// keep the server's name as Name; see NodeStatus.ServerName.
	Name string

	// Dir is the node's directory, relative to the instance directory. Every
	// path in Files starts with it, and Config includes those files by paths
	// relative to it.
	Dir string

	// ServerIndex is the node's 1-based position in its cluster, or 1 for a
	// single server.
	ServerIndex int

	// ClusterIndex is the 1-based position of the node's cluster in a
	// super-cluster, 0 for any other kind.
	ClusterIndex int

	// Cluster names the node's cluster. Empty for a single server.
	Cluster string

	// Listeners are the ports the node accepts connections on, at the
	// endpoints Placement.ListenerAddress returned for them.
	Listeners []Listener

	// Files are the files the node needs, such as its rendered snippets.
	Files []File

	// Config is the node's rendered server config. It is not one of Files: the
	// backend writes it in the node's directory, Dir, under a name of its own
	// choosing, and starts the server from it.
	Config []byte

	// Version is the server version the node runs. It is a neutral version
	// string, such as "2.14.5", which the backend resolves to a binary or an
	// image.
	Version string

	// Persistent are the paths that must survive a revision of the node, as
	// the server sees them, such as its store directory.
	Persistent []string
}

// Listener is a port a node accepts connections on.
type Listener struct {
	// Name is the kind of listener: "client", "cluster", "gateway",
	// "websocket", "mqtt" or "leafnode".
	Name string

	// Endpoint is where the listener is reached, as the node's config renders
	// it.
	Endpoint Endpoint
}

// Link is one node's connection to a listener of another node: the address a
// node dials for one of its route or gateway peers. A node never has a link to
// itself.
//
// Links take effect only where the main template renders them. A custom
// template that wires its own routes or gateways ignores them.
type Link struct {
	// Node names the dialing node.
	Node string

	// Peer names the node being dialed.
	Peer string

	// Kind is the kind of connection: "route" or "gateway".
	Kind string

	// Dial is the endpoint the node dials.
	Dial Endpoint
}

// File is a file an instance needs, held as rendered bytes until the backend
// writes it. A node's config is not a file in this sense; see NodePlan.Config.
type File struct {
	// Path is the file's path relative to the instance directory.
	Path string

	// Data is the file's contents.
	Data []byte

	// Mode is the permission the file is written with.
	Mode fs.FileMode
}
