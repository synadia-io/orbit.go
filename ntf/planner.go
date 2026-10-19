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
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/template"

	"github.com/synadia-io/orbit.go/ntf/agent"
	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/backend"
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
// gateway listener, or a proxy.
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
// every listener address and directory, and gives every node the server
// version version. It renders every file in memory: it writes nothing and
// starts nothing.
//
// When planning fails part way, the error is a *planError naming the kind of
// failure, apart from an unknown spec.Kind. What place reserved is left to the
// backend's Teardown.
func planInstance(spec instanceSpec, id string, place backend.Placement, version string) (*instancePlan, error) {
	short := shortID(id)

	plan := &instancePlan{
		InstancePlan: backend.InstancePlan{
			ID:          id,
			ShortID:     short,
			Kind:        spec.Kind,
			Description: spec.Description,
		},
	}

	fail := func(kind planErrorKind, err error) (*instancePlan, error) {
		return nil, &planError{Kind: kind, Err: err}
	}

	// 1. The TLS material, shared by every node.
	var tlsFiles *tlsInstanceFiles
	if spec.TLS {
		var err error
		tlsFiles, err = planTLS(plan, spec, place.InstanceDir())
		if err != nil {
			return fail(planErrTLSSetup, err)
		}
	}

	// 2. The clusters and nodes, in start order. Each node's Dir is its
	// directory relative to the instance directory, the prefix of its files'
	// paths.
	addNode := func(n backend.NodePlan) {
		n.Version = version
		wrapped := &nodePlan{NodePlan: n}
		plan.nodes = append(plan.nodes, wrapped)
		plan.Nodes = append(plan.Nodes, &wrapped.NodePlan)
	}

	switch spec.Kind {
	case "server":
		addNode(backend.NodePlan{Name: short + "-n1", Dir: "n1", ServerIndex: 1})

	case "cluster":
		cluster := "C_" + short
		plan.Clusters = []string{cluster}
		for i := 1; i <= spec.Servers; i++ {
			dir := fmt.Sprintf("n%d", i)
			addNode(backend.NodePlan{Name: short + "-" + dir, Dir: dir, ServerIndex: i, Cluster: cluster})
		}

	case "super-cluster":
		for c := 1; c <= spec.Clusters; c++ {
			cluster := fmt.Sprintf("SC_%s_%d", short, c)
			plan.Clusters = append(plan.Clusters, cluster)
			for i := 1; i <= spec.Servers; i++ {
				dir := fmt.Sprintf("c%d_s%d", c, i)
				addNode(backend.NodePlan{Name: short + "-" + dir, Dir: dir, ServerIndex: i, ClusterIndex: c, Cluster: cluster})
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

	// 4. The links between the nodes and, with toxics, the proxies they go
	// through. No proxy address is asked for without toxics.
	plan.Links = planLinks(plan)

	if spec.Toxics {
		err = planProxies(plan, place)
		if err != nil {
			return fail(planErrPort, err)
		}
	}

	for _, n := range plan.nodes {
		// 5. The node's store directory, which survives a revision, and its
		// template data.
		storeDir := place.StoreDir(n.Name)
		n.Persistent = []string{storeDir}
		td := planTemplateData(plan, n, spec, filepath.Join(place.InstanceDir(), n.Dir), storeDir)

		// 6. The node's managed TLS snippet.
		if tlsFiles != nil {
			n.TLS = tlsFiles
			n.Files = append(n.Files, managedTLSFile(n))
			td.TLSInclude = filepath.Join("snippets", "_tls_managed.conf")
			td.TLS = &templateTLS{
				CAFile:   tlsFiles.caPath,
				CertFile: tlsFiles.serverCert,
				KeyFile:  tlsFiles.serverKey,
			}
		}

		// 7. The node's snippets and config.
		err = renderNode(n, td, spec.Snippets, spec.MainTemplate)
		if err != nil {
			return nil, err
		}
	}

	// 8. The node the capture proxy fronts.
	if spec.Proxy {
		plan.ProxyNode = plan.nodes[0].Name
	}

	return plan, nil
}

// planRevision returns the next revision of node n, as an update renders it:
// the same identity, listeners, version and persistent paths, the caller's
// snippets rendered against a copy of n's template data in place of its own,
// and a config rendered from mainTemplate. When tlsTimeout is given, the
// revision's managed TLS settings take that timeout and it carries the managed
// TLS snippet as well; n must then have managed TLS settings. n is left as it
// is.
//
// The revision carries only the files it renders. A failure is a *planError of
// kind planErrSnippet or planErrTemplate.
func planRevision(n *nodePlan, snippets map[string]string, mainTemplate string, tlsTimeout *float64) (*nodePlan, error) {
	next := &nodePlan{NodePlan: n.NodePlan, TLS: n.TLS}
	next.Files = nil
	next.Config = nil

	// The TLS settings are shared by every node of the instance, so a new
	// timeout goes into a copy.
	if tlsTimeout != nil {
		cow := *n.TLS
		cow.timeoutSeconds = *tlsTimeout
		next.TLS = &cow
	}

	err := renderNode(next, cloneTemplateData(n.TemplateData), snippets, mainTemplate)
	if err != nil {
		return nil, err
	}

	if tlsTimeout != nil {
		next.Files = append(next.Files, managedTLSFile(next))
	}

	return next, nil
}

// renderNode renders the caller's snippets against td into files appended to
// n.Files, then n.Config from mainTemplate, and keeps td on n as the template
// data the config was rendered with. A failure is a *planError of kind
// planErrSnippet or planErrTemplate.
func renderNode(n *nodePlan, td *templateData, snippets map[string]string, mainTemplate string) error {
	files, err := renderSnippets(td, snippets, filepath.Join(n.Dir, "snippets"))
	if err != nil {
		return &planError{Kind: planErrSnippet, Err: err}
	}
	n.Files = append(n.Files, files...)

	n.Config, err = renderConfig(td, mainTemplate)
	if err != nil {
		return &planError{Kind: planErrTemplate, Err: err}
	}
	n.TemplateData = td

	return nil
}

// managedTLSFile returns node n's managed TLS snippet, rendered from n.TLS.
func managedTLSFile(n *nodePlan) backend.File {
	return backend.File{
		Path: filepath.Join(n.Dir, "snippets", "_tls_managed.conf"),
		Data: renderTLSSnippet(n.TLS),
		Mode: 0600,
	}
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
		backend.File{Path: filepath.Join("tls", "ca.pem"), Data: mat.CAPEM, Mode: 0644},
		backend.File{Path: filepath.Join("tls", "server.crt"), Data: mat.ServerCertPEM, Mode: 0644},
		backend.File{Path: filepath.Join("tls", "server.key"), Data: mat.ServerKeyPEM, Mode: 0600},
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
// Each node's listeners are added to it as they are handed back.
func reserveListeners(plan *instancePlan, spec instanceSpec, place backend.Placement) error {
	snippetListeners := listenersForSnippets(spec.Snippets)

	reserve := func(n *nodePlan, name string) error {
		l, err := place.ListenerAddress(n.Name, name)
		if err != nil {
			return err
		}
		n.Listeners = append(n.Listeners, l)
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
		return reserveNode(plan.nodes[0])

	case "cluster":
		return reserveCluster(plan.nodes)

	default:
		err := reserveAll(plan.nodes, "gateway")
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
func planLinks(plan *instancePlan) []backend.Link {
	var links []backend.Link

	for _, n := range plan.nodes {
		for _, peer := range plan.nodes {
			if peer == n {
				continue
			}

			if peer.Cluster == n.Cluster && n.Cluster != "" {
				l := listenerOf(peer, "cluster")
				links = append(links, backend.Link{Node: n.Name, Peer: peer.Name, Kind: "route", Dial: l.Endpoint})
			}

			if plan.Kind == "super-cluster" {
				l := listenerOf(peer, "gateway")
				links = append(links, backend.Link{Node: n.Name, Peer: peer.Name, Kind: "gateway", Dial: l.Endpoint})
			}
		}
	}

	return links
}

// routeLinkName names the route link of an instance created with toxics from
// node from to node to.
func routeLinkName(from, to string) string {
	return "route:" + from + "->" + to
}

// gatewayLinkName names the gateway link of an instance created with toxics
// from cluster from to node to.
func gatewayLinkName(from, to string) string {
	return "gateway:" + from + "->" + to
}

// clientLinkName names the client link of an instance created with toxics in
// front of node.
func clientLinkName(node string) string {
	return "client:" + node
}

// planProxies asks place for the address of every proxy of plan, an instance
// created with toxics, in this order: one for each route link; one for each
// cluster and each node of another cluster, shared by the cluster's gateway
// links to that node; one for each node's client listener; and last, for a
// cluster or super-cluster, the dead advertise address. Each route link and
// each gateway link to a node of another cluster then dials its proxy. A
// gateway link within a cluster stays direct, since nats-server ignores the
// gateway URLs of its own cluster.
func planProxies(plan *instancePlan, place backend.Placement) error {
	proxy := func(name, kind string) (backend.Endpoint, error) {
		ep, err := place.ProxyAddress(name, kind)
		if err != nil {
			return backend.Endpoint{}, fmt.Errorf("%w: proxy %s: %w", errNoFreePort, name, err)
		}
		return ep, nil
	}

	for i, lk := range plan.Links {
		if lk.Kind != "route" {
			continue
		}

		name := routeLinkName(lk.Node, lk.Peer)
		ep, err := proxy(name, "route")
		if err != nil {
			return err
		}
		plan.proxies = append(plan.proxies, agent.Link{Name: name, Kind: "route", From: lk.Node, To: lk.Peer, Listen: ep, Upstream: lk.Dial})
		plan.Links[i].Dial = ep
	}

	if plan.Kind == "super-cluster" {
		gateways := map[string]backend.Endpoint{}
		for _, cluster := range plan.Clusters {
			for _, peer := range plan.nodes {
				if peer.Cluster == cluster {
					continue
				}

				name := gatewayLinkName(cluster, peer.Name)
				ep, err := proxy(name, "gateway")
				if err != nil {
					return err
				}
				plan.proxies = append(plan.proxies, agent.Link{Name: name, Kind: "gateway", From: cluster, To: peer.Name, Listen: ep, Upstream: listenerOf(peer, "gateway").Endpoint})
				gateways[name] = ep
			}
		}

		clusterOf := map[string]string{}
		for _, n := range plan.nodes {
			clusterOf[n.Name] = n.Cluster
		}
		for i, lk := range plan.Links {
			if lk.Kind != "gateway" {
				continue
			}
			ep, ok := gateways[gatewayLinkName(clusterOf[lk.Node], lk.Peer)]
			if ok {
				plan.Links[i].Dial = ep
			}
		}
	}

	plan.clientProxies = map[string]backend.Endpoint{}
	for _, n := range plan.nodes {
		ep, err := proxy(clientLinkName(n.Name), "client")
		if err != nil {
			return err
		}
		plan.clientProxies[n.Name] = ep
	}

	if plan.Kind != "server" {
		ep, err := proxy("advertise", "advertise")
		if err != nil {
			return err
		}
		plan.deadAdvertise = ep
	}

	return nil
}

// planTemplateData returns the template data node n of plan renders its
// snippets and config with, short of the managed TLS settings. A node's own
// .Routes and .Gateways entries come from its own listeners, every other entry
// from its links. dir is the node's directory and storeDir its store directory.
func planTemplateData(plan *instancePlan, n *nodePlan, spec instanceSpec, dir, storeDir string) *templateData {
	td := defaultTemplateData()

	td.ServerName = n.Name
	td.ShortID = plan.ShortID
	td.InstanceID = plan.ID
	td.ServerIndex = n.ServerIndex
	td.ServerDir = dir
	td.StoreDir = storeDir
	td.LogFile = filepath.Join(dir, "server.log")
	td.Host = "localhost"
	td.AdvertiseHost = spec.AdvertiseHost
	td.ClientPort = listenerOf(n, "client").Endpoint.Port
	td.JetStream = spec.JetStream

	td.Description = plan.Description
	td.Kind = plan.Kind

	if n.Cluster != "" {
		td.ClusterName = n.Cluster
		td.ClusterIndex = n.ClusterIndex
		td.ClusterPort = listenerOf(n, "cluster").Endpoint.Port
		td.Routes = dialAddresses(plan, n, nodesOf(plan, n.Cluster), "cluster", "route")
		td.ClusterSize = spec.Servers
	}

	if plan.Kind == "super-cluster" {
		td.GatewayPort = listenerOf(n, "gateway").Endpoint.Port
		td.Clusters = plan.Clusters
		td.Gateways = map[string][]string{}
		for _, cluster := range plan.Clusters {
			td.Gateways[cluster] = dialAddresses(plan, n, nodesOf(plan, cluster), "gateway", "gateway")
		}
	}

	for _, name := range portBearingSnippets {
		l, ok := findListener(n, name)
		if ok {
			td.Ports[name] = l.Endpoint.Port
		}
	}

	// With toxics, clients are told the node's client proxy, on the advertise
	// host when one is set, and peers the dead address, so gossip does not go
	// around the proxies.
	if spec.Toxics {
		client := plan.clientProxies[n.Name]
		if spec.AdvertiseHost != "" {
			client.Host = spec.AdvertiseHost
		}
		td.ClientAdvertise = client.String()

		if n.Cluster != "" {
			td.ClusterAdvertise = plan.deadAdvertise.String()
		}
		if plan.Kind == "super-cluster" {
			td.GatewayAdvertise = plan.deadAdvertise.String()
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
			addrs = append(addrs, listenerOf(n, listenerName).Endpoint.String())
			continue
		}

		for _, lk := range plan.Links {
			if lk.Node == n.Name && lk.Peer == peer.Name && lk.Kind == linkKind {
				addrs = append(addrs, lk.Dial.String())
				break
			}
		}
	}

	return addrs
}

// nodesOf returns the nodes of plan in the named cluster, in index order.
func nodesOf(plan *instancePlan, cluster string) []*nodePlan {
	var nodes []*nodePlan
	for _, n := range plan.nodes {
		if n.Cluster == cluster {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// findListener returns n's listener of the given name, and whether it has one.
func findListener(n *nodePlan, name string) (backend.Listener, bool) {
	i := slices.IndexFunc(n.Listeners, func(l backend.Listener) bool { return l.Name == name })
	if i < 0 {
		return backend.Listener{}, false
	}
	return n.Listeners[i], true
}

// listenerOf returns n's listener of the given name, or the zero listener when
// it has none.
func listenerOf(n *nodePlan, name string) backend.Listener {
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
func renderSnippets(td *templateData, snippets map[string]string, snippetsDir string) ([]backend.File, error) {
	var files []backend.File

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

		files = append(files, backend.File{Path: filepath.Join(snippetsDir, name+".conf"), Data: out.Bytes(), Mode: 0600})
		td.Snippets[name] = filepath.Join(filepath.Base(snippetsDir), name+".conf")
	}

	return files, nil
}

// checkRouteTLS returns an error wrapping errRouteTLS when cfg, node n's
// parsed config, has a tls block in its cluster or gateway block or in an
// entry of its gateway block's gateways list. Keys match in any case, as the
// server matches them.
func checkRouteTLS(n *nodePlan, cfg map[string]any) error {
	for key, value := range cfg {
		block := strings.ToLower(key)
		if block != "cluster" && block != "gateway" {
			continue
		}

		fields, ok := value.(map[string]any)
		if !ok {
			continue
		}
		for field, fieldValue := range fields {
			if strings.EqualFold(field, "tls") {
				return fmt.Errorf("%w: node %s enables TLS in its %s block", errRouteTLS, n.Name, block)
			}

			if block != "gateway" || !strings.EqualFold(field, "gateways") {
				continue
			}

			entries, ok := fieldValue.([]any)
			if !ok {
				continue
			}
			for i, entry := range entries {
				entryFields, ok := entry.(map[string]any)
				if !ok {
					continue
				}
				for entryField := range entryFields {
					if strings.EqualFold(entryField, "tls") {
						return fmt.Errorf("%w: node %s enables TLS in its gateway entry %s", errRouteTLS, n.Name, gatewayEntryName(entryFields, i))
					}
				}
			}
		}
	}

	return nil
}

// gatewayEntryName returns the name of entry, the fields of the entry at index
// i of a gateways list, or its 1-based position when it has no name.
func gatewayEntryName(entry map[string]any, i int) string {
	for field, value := range entry {
		if !strings.EqualFold(field, "name") {
			continue
		}
		name, ok := value.(string)
		if ok && name != "" {
			return name
		}
	}

	return strconv.Itoa(i + 1)
}

// checkGatewayName returns an error wrapping errGatewayName when node n is in
// a super-cluster and the gateway block of cfg, n's parsed config, has no name
// or a name other than n's planned cluster name, which the gateway gate admits
// a connection by. A config without a gateway block, and a node of any other
// kind of instance, is passed over. Keys match in any case, as the server
// matches them.
func checkGatewayName(n *nodePlan, cfg map[string]any) error {
	if n.TemplateData == nil || n.TemplateData.Kind != "super-cluster" {
		return nil
	}

	for key, value := range cfg {
		if !strings.EqualFold(key, "gateway") {
			continue
		}

		fields, ok := value.(map[string]any)
		if !ok {
			continue
		}

		named := false
		for field, fieldValue := range fields {
			if !strings.EqualFold(field, "name") {
				continue
			}
			named = true

			name := fmt.Sprint(fieldValue)
			if name != n.Cluster {
				return fmt.Errorf("%w: node %s names its gateway %q, want %q", errGatewayName, n.Name, name, n.Cluster)
			}
		}
		if !named {
			return fmt.Errorf("%w: node %s does not name its gateway, want %q", errGatewayName, n.Name, n.Cluster)
		}
	}

	return nil
}
