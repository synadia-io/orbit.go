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
// rendered from. A backend only ever sees the embedded NodePlan.
type nodePlan struct {
	backend.NodePlan

	// TemplateData is the template environment Config was rendered with.
	TemplateData *templateData

	// TLS holds the settings of the node's managed TLS snippet: the paths of
	// the generated material and the verify, handshake first and timeout
	// settings. Nil when the instance has no generated TLS.
	TLS *tlsInstanceFiles
}
