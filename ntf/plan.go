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
	"io/fs"
	"net"
	"path/filepath"
	"strings"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// inProcessHost is the host in-process listeners are reached on. It matches the
// value of .Host in the template environment and the host of every route and
// gateway URL a config lists.
const inProcessHost = "localhost"

// instanceSpec is what a validated create request asks for. The create handlers
// build it after validation, and the planner turns it into an instancePlan. It
// holds no wire types, so planning does not depend on the API.
type instanceSpec struct {
	// Kind is the kind of instance: "server", "cluster" or "super-cluster".
	Kind string

	// Servers is the number of servers in each cluster, or 1 for a single
	// server.
	Servers int

	// Clusters is the number of clusters in a super-cluster, 0 for any other
	// kind.
	Clusters int

	// JetStream enables JetStream on every server.
	JetStream bool

	// Description is the caller's free-form description of the instance.
	Description string

	// Snippets are the caller's snippet bodies, keyed by extension point. Each
	// is a template rendered against the node's template data.
	Snippets map[string]string

	// MainTemplate is the template every node's config is rendered from. It is
	// the built-in template when the request gave none.
	MainTemplate string

	// AdvertiseHost is the resolved client_advertise host for every server.
	// Empty advertises nothing.
	AdvertiseHost string

	// TLS requests generated TLS material for the instance. The other TLS
	// fields apply only when it is set.
	TLS bool

	// TLSMutual requires clients to present the issued client certificate.
	// When false, TLS is one-way and no client certificate is issued.
	TLSMutual bool

	// TLSSANs are the Subject Alternative Names the caller asked for on the
	// server certificate. Empty means the built-in defaults.
	TLSSANs []string

	// TLSHandshakeFirst makes the servers perform the TLS handshake before
	// sending the INFO protocol.
	TLSHandshakeFirst bool

	// TLSTimeout is the TLS handshake timeout in seconds. Zero or less means the
	// managed default of 2 seconds.
	TLSTimeout float64

	// Proxy fronts the instance with the capture proxy.
	Proxy bool

	// StoreCaptures keeps the captures the proxy records. It applies only when
	// Proxy is set.
	StoreCaptures bool
}

// instancePlan is the record of what an instance is: its nodes, how they
// connect and every file they need. The planner builds it from an instanceSpec
// without touching the disk or starting anything, and a runtime writes its
// files and starts its nodes in order.
type instancePlan struct {
	// ID is the instance id.
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

	// Nodes are the instance's nodes in start order.
	Nodes []*nodePlan

	// Links are the connections the nodes make to each other.
	Links []link

	// Files are the files the instance needs outside any one node, such as the
	// generated TLS material.
	Files []file

	// TLS is the material returned to the caller in the create response. Nil
	// when the instance has no generated TLS.
	TLS *api.TLSMaterial

	// ProxyNode names the node whose client port the capture proxy fronts.
	// Empty when the instance has no capture proxy.
	ProxyNode string
}

// dropContents drops the contents of the instance's own files and the TLS
// material from p, keeping the rest of the plan. A runtime calls it once those
// files are written and the TLS material is in the create response, so private
// keys are not held in memory for the life of the instance. Each node's
// contents are dropped by the node's own dropContents.
func (p *instancePlan) dropContents() {
	p.TLS = nil
	for i := range p.Files {
		p.Files[i].Data = nil
	}
}

// dropContents drops the node's rendered config and the contents of its files,
// keeping the rest of its plan. A runtime calls it once the node has started
// and before the node is published, so a published node plan never changes.
func (n *nodePlan) dropContents() {
	n.Config = nil
	for i := range n.Files {
		n.Files[i].Data = nil
	}
}

// nodePlan is the plan for one node of an instance: what it listens on, the
// files it needs and the config it runs.
type nodePlan struct {
	// Name is the node's server name.
	Name string

	// ServerIndex is the node's 1-based position in its cluster, or 1 for a
	// single server.
	ServerIndex int

	// ClusterIndex is the 1-based position of the node's cluster in a
	// super-cluster, 0 for any other kind.
	ClusterIndex int

	// Cluster names the node's cluster. Empty for a single server.
	Cluster string

	// Listeners are the ports the node accepts connections on.
	Listeners []listener

	// Files are the files the node needs, such as its rendered snippets.
	Files []file

	// Config is the node's rendered config. It is not one of Files: the runtime
	// writes it under a random *.cfg name in the node's directory.
	Config []byte

	// TemplateData is the template environment Config was rendered with.
	TemplateData *templateData

	// TLS holds the settings of the node's managed TLS snippet: the paths of
	// the generated material and the verify, handshake first and timeout
	// settings. Nil when the instance has no generated TLS.
	TLS *tlsInstanceFiles

	// Held are the listeners that keep the node's ports reserved until it
	// starts. The runtime closes them just before the node starts. Empty when
	// the placement reserves no ports.
	Held []*net.TCPListener
}

// listener is a port a node accepts connections on.
type listener struct {
	// Name is the kind of listener: "client", "cluster", "gateway",
	// "websocket", "mqtt" or "leafnode".
	Name string

	// Host is the host the listener is reached on.
	Host string

	// Port is the port the listener is reached on.
	Port int
}

// link is one node's connection to a listener of another node: the address a
// node dials for one of its route or gateway peers. A node never has a link to
// itself.
//
// Links take effect only where the main template renders .Routes and
// .Gateways. A custom template that wires its own routes or gateways ignores
// them.
type link struct {
	// Node names the dialing node.
	Node string

	// Peer names the node being dialed.
	Peer string

	// Kind is the kind of connection: "route" or "gateway".
	Kind string

	// Host is the host the node dials.
	Host string

	// Port is the port the node dials.
	Port int
}

// file is a file an instance needs, held as bytes until a runtime writes it. A
// node's config is not a file in this sense; see nodePlan.Config.
type file struct {
	// Path is the file's path relative to the instance directory.
	Path string

	// Data is the file's contents.
	Data []byte

	// Mode is the permission the file is written with.
	Mode fs.FileMode
}

// placement supplies the planner with everything that depends on where an
// instance runs: the address of each listener and the directories its files go
// in.
type placement interface {
	// listenerAddress returns the address of the named listener of the named
	// node. A placement that reserves ports also returns the listener holding
	// the port, which the caller must close just before the node binds the
	// port. It returns a nil listener otherwise.
	listenerAddress(node, name string) (listener, *net.TCPListener, error)

	// instanceDir returns the instance directory.
	instanceDir() string

	// nodeDir returns the directory of the named node. The node must be named
	// as the planner names it.
	nodeDir(node string) string
}

// inProcessPlacement places an instance on this host, for nodes that run in the
// service's own process. Ports come from the service's port range and are
// recorded on the instance, so they return to the range when the instance is
// torn down. Directories live under the service's directory.
type inProcessPlacement struct {
	svc  *Service
	inst *instance
}

// newInProcessPlacement returns the in-process placement for inst, which must
// have been registered with newInstance.
func (s *Service) newInProcessPlacement(inst *instance) *inProcessPlacement {
	return &inProcessPlacement{svc: s, inst: inst}
}

// listenerAddress reserves a port from the service's port range and returns it
// on localhost, with the listener holding it. The node and listener names do
// not affect the port. An exhausted range returns an error wrapping
// ErrPortRangeExhausted.
func (p *inProcessPlacement) listenerAddress(node, name string) (listener, *net.TCPListener, error) {
	port, ln, err := p.svc.reservePort(p.inst)
	if err != nil {
		return listener{}, nil, err
	}

	return listener{Name: name, Host: inProcessHost, Port: port}, ln, nil
}

// instanceDir returns <Dir>/<id>.
func (p *inProcessPlacement) instanceDir() string {
	return p.inst.RootDir
}

// nodeDir returns the node's directory under the instance directory: n<i> for
// a single server or a cluster node, c<c>_s<i> for a super-cluster node. It is
// the node's name without the "<short id>-" prefix the planner gives it.
func (p *inProcessPlacement) nodeDir(node string) string {
	return filepath.Join(p.instanceDir(), strings.TrimPrefix(node, shortID(p.inst.ID)+"-"))
}

var _ placement = (*inProcessPlacement)(nil)
