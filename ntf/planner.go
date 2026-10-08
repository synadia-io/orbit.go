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
	"bytes"
	"errors"
	"fmt"
	"maps"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"text/template"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// planErrorKind is the kind of failure planning an instance hit. The create
// handlers answer each kind with its own response code.
type planErrorKind int

const (
	// planErrTLSSetup means the TLS material could not be generated. Answered
	// with 008.
	planErrTLSSetup planErrorKind = iota + 1

	// planErrTLSSnippet means the managed TLS snippet could not be produced.
	// Answered with 008. Rendering it cannot fail, so the planner never returns
	// this kind; it is the kind of a failed write of the snippet.
	planErrTLSSnippet

	// planErrPort means a listener address could not be had. Answered with 006.
	planErrPort

	// planErrSnippet means a caller's snippet failed to parse or render.
	// Answered with 002.
	planErrSnippet

	// planErrTemplate means the main template failed to parse or render.
	// Answered with 002.
	planErrTemplate
)

// errNoFreePort is the cause of a planErrPort failure for a client, cluster or
// gateway listener.
var errNoFreePort = errors.New("could not get free port")

// planError is a failure to plan an instance. Its message is the message the
// create handlers answer the kind with.
type planError struct {
	// Kind is the kind of failure.
	Kind planErrorKind

	// Err is the cause.
	Err error
}

// Error returns the kind's message prefix followed by the cause. A
// planErrPort failure has no prefix: its cause already names the port.
func (e *planError) Error() string {
	switch e.Kind {
	case planErrTLSSetup:
		return "TLS setup failed: " + e.Err.Error()
	case planErrTLSSnippet:
		return "TLS snippet failed: " + e.Err.Error()
	case planErrSnippet:
		return "Snippet failure: " + e.Err.Error()
	case planErrTemplate:
		return "Template parse failure: " + e.Err.Error()
	default:
		return e.Err.Error()
	}
}

// Unwrap returns the cause.
func (e *planError) Unwrap() error {
	return e.Err
}

// planInstance turns spec into the plan of the instance id, asking place for
// every listener address and directory. It renders every file in memory: it
// writes nothing and starts nothing.
//
// The node listeners place hands back stay held on the plan's nodes. When
// planning fails part way, every one of them is closed and the error is a
// *planError naming the kind of failure, apart from an unknown spec.Kind.
func planInstance(spec instanceSpec, id string, place placement) (*instancePlan, error) {
	short := shortID(id)

	plan := &instancePlan{
		ID:          id,
		ShortID:     short,
		Kind:        spec.Kind,
		Description: spec.Description,
	}

	fail := func(kind planErrorKind, err error) (*instancePlan, error) {
		for _, n := range plan.Nodes {
			closeListeners(n.Held)
		}
		return nil, &planError{Kind: kind, Err: err}
	}

	// 1. The TLS material, shared by every node.
	var tlsFiles *tlsInstanceFiles
	if spec.TLS {
		var err error
		tlsFiles, err = planTLS(plan, spec, place.instanceDir())
		if err != nil {
			return fail(planErrTLSSetup, err)
		}
	}

	// 2. The clusters and nodes, in start order. dirs holds each node's
	// directory name, the prefix of its files' paths.
	var dirs []string
	addNode := func(n *nodePlan, dir string) {
		plan.Nodes = append(plan.Nodes, n)
		dirs = append(dirs, dir)
	}

	switch spec.Kind {
	case "server":
		addNode(&nodePlan{Name: short + "-n1", ServerIndex: 1}, "n1")

	case "cluster":
		cluster := "C_" + short
		plan.Clusters = []string{cluster}
		for i := 1; i <= spec.Servers; i++ {
			dir := fmt.Sprintf("n%d", i)
			addNode(&nodePlan{Name: short + "-" + dir, ServerIndex: i, Cluster: cluster}, dir)
		}

	case "super-cluster":
		for c := 1; c <= spec.Clusters; c++ {
			cluster := fmt.Sprintf("SC_%s_%d", short, c)
			plan.Clusters = append(plan.Clusters, cluster)
			for i := 1; i <= spec.Servers; i++ {
				dir := fmt.Sprintf("c%d_s%d", c, i)
				addNode(&nodePlan{Name: short + "-" + dir, ServerIndex: i, ClusterIndex: c, Cluster: cluster}, dir)
			}
		}

	default:
		return nil, fmt.Errorf("unknown instance kind %q", spec.Kind)
	}

	// 3. Every listener address, in the order the create handlers have always
	// reserved them.
	err := reserveListeners(plan, spec, place)
	if err != nil {
		return fail(planErrPort, err)
	}

	// 4. The links between the nodes.
	plan.Links = planLinks(plan)

	for i, n := range plan.Nodes {
		// 5. The node's template data.
		td := planTemplateData(plan, n, spec, place.nodeDir(n.Name))

		// 6. The node's snippets.
		if tlsFiles != nil {
			n.TLS = tlsFiles
			n.Files = append(n.Files, file{
				Path: filepath.Join(dirs[i], "snippets", "_tls_managed.conf"),
				Data: renderTLSSnippet(tlsFiles),
				Mode: 0600,
			})
			td.TLSInclude = filepath.Join("snippets", "_tls_managed.conf")
			td.TLS = &templateTLS{
				CAFile:   tlsFiles.caPath,
				CertFile: tlsFiles.serverCert,
				KeyFile:  tlsFiles.serverKey,
			}
		}

		snippets, err := renderSnippets(td, spec.Snippets, filepath.Join(dirs[i], "snippets"))
		if err != nil {
			return fail(planErrSnippet, err)
		}
		n.Files = append(n.Files, snippets...)

		// 7. The node's config.
		n.Config, err = renderConfig(td, spec.MainTemplate)
		if err != nil {
			return fail(planErrTemplate, err)
		}
		n.TemplateData = td
	}

	// 8. The node the capture proxy fronts.
	if spec.Proxy {
		plan.ProxyNode = plan.Nodes[0].Name
	}

	return plan, nil
}

// planTLS generates the instance's TLS material with the SANs spec asks for and
// its advertise host, adds the CA, server certificate and server key to the
// plan's files under tls/, and keeps the material for the create response on
// the plan. It returns the settings of the managed TLS snippet, with the paths
// of the material under instanceDir.
func planTLS(plan *instancePlan, spec instanceSpec, instanceDir string) (*tlsInstanceFiles, error) {
	mat, err := generateTLSMaterial(effectiveSANs(spec.TLSSANs, spec.AdvertiseHost), spec.TLSMutual)
	if err != nil {
		return nil, fmt.Errorf("tls material: %w", err)
	}

	plan.Files = append(plan.Files,
		file{Path: filepath.Join("tls", "ca.pem"), Data: mat.CAPEM, Mode: 0644},
		file{Path: filepath.Join("tls", "server.crt"), Data: mat.ServerCertPEM, Mode: 0644},
		file{Path: filepath.Join("tls", "server.key"), Data: mat.ServerKeyPEM, Mode: 0600},
	)

	plan.TLS = &api.TLSMaterial{CAPEM: string(mat.CAPEM)}
	if spec.TLSMutual {
		plan.TLS.ClientCertPEM = string(mat.ClientCertPEM)
		plan.TLS.ClientKeyPEM = string(mat.ClientKeyPEM)
	}

	tlsDir := filepath.Join(instanceDir, "tls")

	return &tlsInstanceFiles{
		caPath:         filepath.Join(tlsDir, "ca.pem"),
		serverCert:     filepath.Join(tlsDir, "server.crt"),
		serverKey:      filepath.Join(tlsDir, "server.key"),
		mutual:         spec.TLSMutual,
		handshakeFirst: spec.TLSHandshakeFirst,
		timeoutSeconds: spec.TLSTimeout,
	}, nil
}

// reserveListeners asks place for the address of every listener of every node
// of plan, in this order:
//
//   - a server: its client listener, then its snippet listeners;
//   - a cluster: every node's cluster listener, then node by node its client
//     listener and snippet listeners;
//   - a super-cluster: every gateway listener of every cluster, then cluster by
//     cluster that cluster's cluster listeners followed by its nodes' client and
//     snippet listeners, node by node.
//
// Each node's listeners and held listeners are added to it as they are handed
// back.
func reserveListeners(plan *instancePlan, spec instanceSpec, place placement) error {
	snippetListeners := listenersForSnippets(spec.Snippets)

	reserve := func(n *nodePlan, name string) error {
		l, ln, err := place.listenerAddress(n.Name, name)
		if err != nil {
			return err
		}
		n.Listeners = append(n.Listeners, l)
		if ln != nil {
			n.Held = append(n.Held, ln)
		}
		return nil
	}

	reserveNode := func(n *nodePlan) error {
		err := reserve(n, "client")
		if err != nil {
			return fmt.Errorf("%w: %w", errNoFreePort, err)
		}
		for _, name := range snippetListeners {
			err := reserve(n, name)
			if err != nil {
				return fmt.Errorf("listener %q: %w", name, err)
			}
		}
		return nil
	}

	reserveAll := func(nodes []*nodePlan, name string) error {
		for _, n := range nodes {
			err := reserve(n, name)
			if err != nil {
				return fmt.Errorf("%w: %w", errNoFreePort, err)
			}
		}
		return nil
	}

	reserveCluster := func(nodes []*nodePlan) error {
		err := reserveAll(nodes, "cluster")
		if err != nil {
			return err
		}
		for _, n := range nodes {
			err := reserveNode(n)
			if err != nil {
				return err
			}
		}
		return nil
	}

	switch plan.Kind {
	case "server":
		return reserveNode(plan.Nodes[0])

	case "cluster":
		return reserveCluster(plan.Nodes)

	default:
		err := reserveAll(plan.Nodes, "gateway")
		if err != nil {
			return err
		}
		for _, cluster := range plan.Clusters {
			err := reserveCluster(nodesOf(plan, cluster))
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// planLinks returns the links of plan's nodes: for each node, a route link to
// every other node of its cluster and, in a super-cluster, a gateway link to
// every other node of every cluster. Each dials the peer's cluster or gateway
// listener.
func planLinks(plan *instancePlan) []link {
	var links []link

	for _, n := range plan.Nodes {
		for _, peer := range plan.Nodes {
			if peer == n {
				continue
			}

			if peer.Cluster == n.Cluster && n.Cluster != "" {
				l := listenerOf(peer, "cluster")
				links = append(links, link{Node: n.Name, Peer: peer.Name, Kind: "route", Host: l.Host, Port: l.Port})
			}

			if plan.Kind == "super-cluster" {
				l := listenerOf(peer, "gateway")
				links = append(links, link{Node: n.Name, Peer: peer.Name, Kind: "gateway", Host: l.Host, Port: l.Port})
			}
		}
	}

	return links
}

// planTemplateData returns the template data node n of plan renders its
// snippets and config with, short of the managed TLS settings. A node's own
// .Routes and .Gateways entries come from its own listeners, every other entry
// from its links.
func planTemplateData(plan *instancePlan, n *nodePlan, spec instanceSpec, dir string) *templateData {
	td := defaultTemplateData()

	td.ServerName = n.Name
	td.ShortID = plan.ShortID
	td.InstanceID = plan.ID
	td.ServerIndex = n.ServerIndex
	td.ServerDir = dir
	td.StoreDir = dir
	td.LogFile = filepath.Join(dir, "server.log")
	td.Host = "localhost"
	td.AdvertiseHost = spec.AdvertiseHost
	td.ClientPort = listenerOf(n, "client").Port
	td.JetStream = spec.JetStream

	td.Description = plan.Description
	td.Kind = plan.Kind

	if n.Cluster != "" {
		td.ClusterName = n.Cluster
		td.ClusterIndex = n.ClusterIndex
		td.ClusterPort = listenerOf(n, "cluster").Port
		td.Routes = dialAddresses(plan, n, nodesOf(plan, n.Cluster), "cluster", "route")
		td.ClusterSize = spec.Servers
	}

	if plan.Kind == "super-cluster" {
		td.GatewayPort = listenerOf(n, "gateway").Port
		td.Clusters = plan.Clusters
		td.Gateways = map[string][]string{}
		for _, cluster := range plan.Clusters {
			td.Gateways[cluster] = dialAddresses(plan, n, nodesOf(plan, cluster), "gateway", "gateway")
		}
	}

	for _, name := range portBearingSnippets {
		l, ok := findListener(n, name)
		if ok {
			td.Ports[name] = l.Port
		}
	}

	return td
}

// dialAddresses returns the host:port n reaches each of peers on, in order: its
// own listener of the given name when n is one of them, and the dial address of
// its link of the given kind for every other peer.
func dialAddresses(plan *instancePlan, n *nodePlan, peers []*nodePlan, listenerName, linkKind string) []string {
	addrs := make([]string, 0, len(peers))

	for _, peer := range peers {
		if peer == n {
			l := listenerOf(n, listenerName)
			addrs = append(addrs, net.JoinHostPort(l.Host, strconv.Itoa(l.Port)))
			continue
		}

		for _, lk := range plan.Links {
			if lk.Node == n.Name && lk.Peer == peer.Name && lk.Kind == linkKind {
				addrs = append(addrs, net.JoinHostPort(lk.Host, strconv.Itoa(lk.Port)))
				break
			}
		}
	}

	return addrs
}

// nodesOf returns the nodes of plan in the named cluster, in index order.
func nodesOf(plan *instancePlan, cluster string) []*nodePlan {
	var nodes []*nodePlan
	for _, n := range plan.Nodes {
		if n.Cluster == cluster {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// findListener returns n's listener of the given name, and whether it has one.
func findListener(n *nodePlan, name string) (listener, bool) {
	i := slices.IndexFunc(n.Listeners, func(l listener) bool { return l.Name == name })
	if i < 0 {
		return listener{}, false
	}
	return n.Listeners[i], true
}

// listenerOf returns n's listener of the given name, or the zero listener when
// it has none.
func listenerOf(n *nodePlan, name string) listener {
	l, _ := findListener(n, name)
	return l
}

// renderTLSSnippet renders the managed TLS snippet for files: a tls{} block
// with the absolute paths of the generated material. A timeout of zero or less
// renders the managed default of 2 seconds. client_advertise is owned by the
// main template, so the snippet does not emit it.
func renderTLSSnippet(files *tlsInstanceFiles) []byte {
	verify := "false"
	if files.mutual {
		verify = "true"
	}
	handshakeFirst := ""
	if files.handshakeFirst {
		handshakeFirst = "\n    handshake_first: true"
	}
	secs := files.timeoutSeconds
	if secs <= 0 {
		secs = 2
	}
	timeout := strconv.FormatFloat(secs, 'f', -1, 64)

	return fmt.Appendf(nil, `tls {
    cert_file: "%s"
    key_file:  "%s"
    ca_file:   "%s"
    verify:    %s
    timeout:   %s%s
}
`, files.serverCert, files.serverKey, files.caPath, verify, timeout, handshakeFirst)
}

// renderSnippets renders each of the caller's snippets through text/template
// against td, in name order, into a file at <snippetsDir>/<name>.conf with mode
// 0600, where snippetsDir is relative to the instance directory. It sets
// td.Snippets[name] to the snippet's include path relative to the node's
// directory, where its config lives: snippets/<name>.conf.
func renderSnippets(td *templateData, snippets map[string]string, snippetsDir string) ([]file, error) {
	var files []file

	for _, name := range slices.Sorted(maps.Keys(snippets)) {
		t, err := template.New("snippet-" + name).Parse(snippets[name])
		if err != nil {
			return nil, fmt.Errorf("snippet %q parse: %w", name, err)
		}

		out := bytes.NewBuffer(nil)
		err = t.Execute(out, td)
		if err != nil {
			return nil, fmt.Errorf("snippet %q render: %w", name, err)
		}

		files = append(files, file{Path: filepath.Join(snippetsDir, name+".conf"), Data: out.Bytes(), Mode: 0600})
		td.Snippets[name] = filepath.Join(filepath.Base(snippetsDir), name+".conf")
	}

	return files, nil
}
