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
	"net"
	"path/filepath"
	"strings"

	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/backend"
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
//
// It embeds the plan a backend runs and adds what only the service uses. A
// backend only ever sees the embedded InstancePlan.
type instancePlan struct {
	backend.InstancePlan

	// nodes are the instance's nodes in start order, as the planner builds
	// them. nodes[i] embeds the node plan InstancePlan.Nodes[i] points at.
	nodes []*nodePlan

	// TLS is the material returned to the caller in the create response. Nil
	// when the instance has no generated TLS.
	TLS *api.TLSMaterial
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
// files it needs and the config it runs. The in-process runtime writes Config
// under a random *.cfg name in the node's directory.
//
// It embeds the node plan a backend runs and adds the inputs Config was
// rendered from and the listeners holding the node's reserved ports. A backend
// only ever sees the embedded NodePlan.
type nodePlan struct {
	backend.NodePlan

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

// portHolder is implemented by a placement that reserves each port it hands
// out by holding a listener on it. The planner takes each held listener as soon
// as ListenerAddress returns, and keeps it on the node until the node starts.
type portHolder interface {
	// takeHeld returns the listener holding the port the last ListenerAddress
	// call for the named listener of the named node reserved, and forgets it.
	// It returns nil when it holds none.
	takeHeld(node, listener string) *net.TCPListener
}

// inProcessPlacement places an instance on this host, for nodes that run in the
// service's own process. Ports come from the service's port range and are
// recorded on the instance, so they return to the range when the instance is
// torn down. Directories live under the service's directory.
type inProcessPlacement struct {
	svc  *Service
	inst *instance

	// held are the listeners holding the ports ListenerAddress reserved, keyed
	// by node and listener name, until takeHeld hands them over.
	held map[[2]string]*net.TCPListener
}

// newInProcessPlacement returns the in-process placement for inst, which must
// have been registered with newInstance.
func (s *Service) newInProcessPlacement(inst *instance) *inProcessPlacement {
	return &inProcessPlacement{svc: s, inst: inst, held: map[[2]string]*net.TCPListener{}}
}

// ListenerAddress reserves a port from the service's port range and returns it
// on localhost. The listener holding the port is kept for takeHeld. The node
// and listener names do not affect the port. An exhausted range returns an
// error wrapping ErrPortRangeExhausted.
func (p *inProcessPlacement) ListenerAddress(node, name string) (backend.Listener, error) {
	port, ln, err := p.svc.reservePort(p.inst)
	if err != nil {
		return backend.Listener{}, err
	}

	p.held[[2]string{node, name}] = ln

	return backend.Listener{Name: name, Endpoint: backend.Endpoint{Host: inProcessHost, Port: port}}, nil
}

// takeHeld returns the listener holding the port ListenerAddress reserved for
// the named listener of the named node, and forgets it.
func (p *inProcessPlacement) takeHeld(node, name string) *net.TCPListener {
	key := [2]string{node, name}
	ln := p.held[key]
	delete(p.held, key)
	return ln
}

// InstanceDir returns <Dir>/<id>.
func (p *inProcessPlacement) InstanceDir() string {
	return p.inst.RootDir
}

// StoreDir returns the node's directory under the instance directory: n<i> for
// a single server or a cluster node, c<c>_s<i> for a super-cluster node. It is
// the node's name without the "<short id>-" prefix the planner gives it.
func (p *inProcessPlacement) StoreDir(node string) string {
	return filepath.Join(p.InstanceDir(), strings.TrimPrefix(node, shortID(p.inst.ID)+"-"))
}

var (
	_ backend.Placement = (*inProcessPlacement)(nil)
	_ portHolder        = (*inProcessPlacement)(nil)
)
