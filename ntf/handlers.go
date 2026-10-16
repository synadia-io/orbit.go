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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/micro"

	"github.com/synadia-io/orbit.go/ntf/agent"
	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/backend"
	"github.com/synadia-io/orbit.go/ntf/events"
)

// Sanity limits on how much a single request may ask for. A super-cluster
// multiplies them, so an uncapped request for a thousand of each would try to
// start a million servers and take the host down. They are guard rails against a
// typo or a runaway generator, not a considered capacity limit.
const (
	// maxServersPerCluster caps the nodes in one cluster.
	maxServersPerCluster = 20
	// maxClusters caps the clusters in one super-cluster.
	maxClusters = 10
)

// allowedSnippetKeys is the closed set of snippet extension points exposed by
// the built-in main template. Keys map 1:1 to {{ if .Snippets.<key> }} guards
// in serverConfigTemplate; passing any other key is rejected at request time.
var allowedSnippetKeys = []string{
	"accounts",
	"system_account",
	"authorization",
	"tls",
	"websocket",
	"mqtt",
	"leafnode",
	"jetstream",
	"top",
}

// portBearingSnippets are snippet keys whose body normally needs an extra
// listener port. When such a key is present in a create request, the service
// auto-reserves a TCP port under the same name and exposes it as
// .Ports.<name> so the snippet can render `port: {{ .Ports.<name> }}`.
var portBearingSnippets = []string{"websocket", "mqtt", "leafnode"}

func validateSnippetKeys(snippets map[string]string) error {
	for k := range snippets {
		if !slices.Contains(allowedSnippetKeys, k) {
			return fmt.Errorf("unknown snippet key %q (allowed: %s)", k, strings.Join(allowedSnippetKeys, ", "))
		}
	}
	return nil
}

// validateJetStreamSnippet rejects a jetstream snippet when JetStream is not
// enabled for the instance. The snippet is rendered inside the
// {{ if .JetStream }} guard in serverConfigTemplate, so with JetStream off it
// would be written to disk but silently dropped from the config — fail loudly
// instead.
func validateJetStreamSnippet(snippets map[string]string, jsEnabled bool) error {
	if _, ok := snippets["jetstream"]; ok && !jsEnabled {
		return fmt.Errorf("jetstream snippet requires JetStream to be enabled on the instance")
	}
	return nil
}

// listenersForSnippets returns the listener names to reserve for the
// port-bearing snippets present in snippets, in a deterministic order.
func listenersForSnippets(snippets map[string]string) []string {
	if len(snippets) == 0 {
		return nil
	}
	out := make([]string, 0, len(portBearingSnippets))
	for _, k := range portBearingSnippets {
		if _, ok := snippets[k]; ok {
			out = append(out, k)
		}
	}
	return out
}

// validateListenerPortSet enforces the update invariant that the set of
// port-bearing snippet keys present in the update request must equal the
// set of listener ports reserved at create time (tracked in current).
func validateListenerPortSet(newSnippets map[string]string, current map[string]int) error {
	wantKeys := listenersForSnippets(newSnippets)
	haveKeys := slices.Sorted(maps.Keys(current))

	var missing, extra []string
	for _, k := range haveKeys {
		if !slices.Contains(wantKeys, k) {
			missing = append(missing, k)
		}
	}
	for _, k := range wantKeys {
		if !slices.Contains(haveKeys, k) {
			extra = append(extra, k)
		}
	}

	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}
	return fmt.Errorf("listener-port snippet set changed (missing from new payload: %v, added in new payload: %v)", missing, extra)
}

var serverConfigTemplate = `
{{ if .Snippets.top }}include "{{ .Snippets.top }}"{{ end }}
server_name: "{{ .ServerName }}"
port: {{ .ClientPort }}
{{ if .ClientAdvertise }}
client_advertise: "{{ .ClientAdvertise }}"
{{ else if .AdvertiseHost }}
client_advertise: "{{ hostport .AdvertiseHost .ClientPort }}"
{{ end }}
{{ if .LogFile }}
log_file: "{{ .LogFile }}"
{{ end }}

{{ if .JetStream }}
jetstream {
	enabled: true
	store_dir: "{{ .StoreDir }}"
	{{ if .Snippets.jetstream }}include "{{ .Snippets.jetstream }}"{{ end }}
}
{{ end }}

{{ if and .ClusterName .Routes}}
cluster {
	name: "{{ .ClusterName }}"
	port: {{ .ClusterPort }}
{{- if .ClusterAdvertise }}
	advertise: "{{ .ClusterAdvertise }}"
{{- end }}
	routes: [
{{ range .Routes }}
"nats://{{ . }}"
{{ end }}
	]
}
{{ end }}

{{ if and .Gateways .GatewayPort .ClusterName }}
gateway {
	name: "{{ .ClusterName }}"
	port: {{ .GatewayPort }}
{{- if .GatewayAdvertise }}
	advertise: "{{ .GatewayAdvertise }}"
{{- end }}
	gateways: [
{{ range $cluster, $urls := .Gateways }}
		{ name: "{{ $cluster }}", urls: [
{{- 		range $urls }}
		"nats://{{- . -}}"
{{ end -}}
		]}
{{ end }}
	]
}
{{ end }}

{{ if .Snippets.tls }}include "{{ .Snippets.tls }}"{{ end }}
{{ if .TLSInclude }}include "{{ .TLSInclude }}"{{ end }}
{{ if .Snippets.websocket }}include "{{ .Snippets.websocket }}"{{ end }}
{{ if .Snippets.mqtt }}include "{{ .Snippets.mqtt }}"{{ end }}
{{ if .Snippets.leafnode }}include "{{ .Snippets.leafnode }}"{{ end }}

{{ if .Snippets.system_account }}include "{{ .Snippets.system_account }}"{{ else }}system_account: "$SYS"{{ end }}
{{ if .Snippets.authorization }}include "{{ .Snippets.authorization }}"{{ else }}no_auth_user: user1{{ end }}

{{ if .Snippets.accounts }}include "{{ .Snippets.accounts }}"{{ else }}accounts {
	USERS1 {
		users = [ { user: "user1", pass: "password" } ]
		jetstream: {{ .JetStream }}
	}
	USERS2 {
		users = [ { user: "user2", pass: "password" } ]
		jetstream: {{ .JetStream }}
	}
	USERS3 {
		users = [ { user: "user3", pass: "password" } ]
		jetstream: {{ .JetStream }}
	}
	USERS4 {
		users = [ { user: "user4", pass: "password" } ]
		jetstream: {{ .JetStream }}
	}
	USERS5 {
		users = [ { user: "user5", pass: "password" } ]
		jetstream: {{ .JetStream }}
	}
	$SYS { users = [ { user: "system", pass: "password" } ] }
}{{ end }}`

// renderConfig renders mainTemplate against td using text/template. The
// returned bytes are written verbatim to the per-server config file.
func renderConfig(td *templateData, mainTemplate string) ([]byte, error) {
	out := bytes.NewBuffer(nil)

	t, err := template.New("configuration").Funcs(template.FuncMap{
		// hostport joins a host and port, bracketing IPv6 literals so the
		// rendered value is always a valid NATS host:port (e.g. "[::1]:4222").
		"hostport": func(host string, port int) string {
			return net.JoinHostPort(host, strconv.Itoa(port))
		},
	}).Parse(mainTemplate)
	if err != nil {
		return nil, err
	}

	if err := t.Execute(out, td); err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

// closeListeners safely closes any still-open listeners and clears the slice.
// Used both right before binding the server (handing the port over) and on
// rollback paths.
func closeListeners(listeners []*net.TCPListener) {
	for _, l := range listeners {
		if l != nil {
			l.Close()
		}
	}
}

// validateTLSSnippetRefs rejects snippets that reference the .TLS template
// field when generated TLS is not active, turning what would otherwise be an
// opaque nil-pointer render error into an actionable message.
func validateTLSSnippetRefs(snippets map[string]string, tlsActive bool) error {
	if tlsActive {
		return nil
	}
	for name, body := range snippets {
		if strings.Contains(body, ".TLS") {
			return fmt.Errorf("snippet %q references .TLS.* but generated TLS is not set: add WithGeneratedTLS", name)
		}
	}
	return nil
}

// validateTLSRequest enforces the two mutually-exclusive constraints around
// CreateRequest.TLS: it cannot coexist with a user-supplied "tls" snippet, and
// when paired with a custom template the template must opt into the managed
// TLS include by referencing .TLSInclude.
func validateTLSRequest(opts *api.TLSOptions, snippets map[string]string, customTemplate string) error {
	if err := validateTLSSnippetRefs(snippets, opts != nil); err != nil {
		return err
	}
	if opts == nil {
		return nil
	}
	if v, ok := snippets["tls"]; ok && v != "" {
		return fmt.Errorf("WithGeneratedTLS and WithTLSSnippet are mutually exclusive: drop one")
	}
	if customTemplate != "" && !strings.Contains(customTemplate, "TLSInclude") {
		return fmt.Errorf("WithTemplate must reference {{.TLSInclude}} when WithGeneratedTLS is set")
	}
	return nil
}

// toxicsTemplateRefs are the template variables that carry the proxies of an
// instance created with toxics, by kind of instance. A custom template must
// reference each, or the instance's traffic goes around the proxies.
var toxicsTemplateRefs = map[string][]string{
	"server":        {".ClientAdvertise"},
	"cluster":       {".Routes", ".ClusterAdvertise", ".ClientAdvertise"},
	"super-cluster": {".Routes", ".ClusterAdvertise", ".ClientAdvertise", ".Gateways", ".GatewayAdvertise"},
}

// validateToxicsTemplate refuses customTemplate, the custom template of an
// instance of the given kind created with toxics, unless its text references
// every variable of toxicsTemplateRefs, naming those it lacks. An empty
// customTemplate is the built-in template, which references them all.
func validateToxicsTemplate(kind, customTemplate string) error {
	if customTemplate == "" {
		return nil
	}

	var missing []string
	for _, ref := range toxicsTemplateRefs[kind] {
		if !strings.Contains(customTemplate, ref) {
			missing = append(missing, ref)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	return fmt.Errorf("WithTemplate must reference %s when toxics is set", strings.Join(missing, ", "))
}

// toxicsRefusal returns the response code and message a create of an
// instance of the given kind with toxics is refused with, or two empty
// strings when it is not refused: 007 for a custom template that does not
// reference the variables that carry the proxies, 014 when no agent is
// selected, and 018 when the agent does not declare the toxics capability.
func (s *Service) toxicsRefusal(kind, customTemplate string) (string, string) {
	err := validateToxicsTemplate(kind, customTemplate)
	if err != nil {
		return "007", err.Error()
	}

	if s.agents == nil {
		return "014", "toxics is not configured on this service"
	}

	if !s.agentDeclares(agent.CapabilityToxics) {
		return "018", fmt.Sprintf("the %q agent does not declare the %q capability", s.agentName, agent.CapabilityToxics)
	}

	return "", ""
}

// tlsInstanceFiles holds on-disk paths for the shared TLS artifacts written
// once per instance. The server cert/key are reused by every node in the
// instance — SANs are caller-supplied, so a single leaf covers them all.
type tlsInstanceFiles struct {
	caPath         string
	serverCert     string
	serverKey      string
	mutual         bool
	handshakeFirst bool
	timeoutSeconds float64
}

// templateTLS exposes the absolute paths of the generated TLS material to
// config snippets via the .TLS template field, so a listener snippet
// (websocket, leafnode, mqtt, ...) can wire the managed certs into its own
// tls{} block — e.g. websocket TLS. Populated only when generated TLS is
// requested. The *File suffix marks these as on-disk paths, distinct from the
// PEM strings on api.TLSMaterial.
type templateTLS struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

// effectiveAdvertiseHost resolves the client_advertise host for a server. An
// operator-supplied --advertise host always wins. Absent that, generated-TLS
// instances default to "localhost" so the advertised host stays within the
// generated cert's SANs and cross-server TLS discovery verifies; non-TLS
// instances advertise nothing and let the server detect its own address.
func effectiveAdvertiseHost(configured string, tls *api.TLSOptions) string {
	if configured != "" {
		return configured
	}
	if tls != nil {
		return "localhost"
	}
	return ""
}

// effectiveSANs returns the SAN list for the generated server cert: the
// caller-supplied SANs (or the built-in defaults when none were given) with the
// advertised host appended when it is not already present, so TLS discovery to
// the advertised address verifies. The input slices are never mutated.
func effectiveSANs(requested []string, advertiseHost string) []string {
	base := requested
	if len(base) == 0 {
		base = defaultTLSSANs
	}
	sans := slices.Clone(base)
	if advertiseHost != "" && !slices.Contains(sans, advertiseHost) {
		sans = append(sans, advertiseHost)
	}
	return sans
}

// createAttempts is how many times a create runs when one of its servers or its
// agent could not bind a port. The service reserves every port and hands it over
// just before the server binds it; another program can take the port in that gap,
// and a retry with freshly reserved ports gets past it.
const createAttempts = 3

// createAttempt runs one attempt at a create request. It answers req itself,
// unless a server or the agent could not listen and final is false: it then rolls
// back everything the attempt started and returns true to have the create run
// again.
type createAttempt func(req micro.Request, final bool) (retry bool)

// retryCreate runs create until it answers req, at most createAttempts times.
func retryCreate(req micro.Request, create createAttempt) {
	for attempt := range createAttempts {
		if !create(req, attempt == createAttempts-1) {
			return
		}
	}
}

// retryAfter reports whether a create attempt that failed to start a server or the
// agent with err should run again: only a server or an agent that could not
// listen, and not on the final attempt. The caller must have rolled the attempt
// back.
func (s *Service) retryAfter(err error, final bool, instanceID string) bool {
	if final {
		return false
	}
	if !errors.Is(err, ErrServerListen) && !errors.Is(err, agent.ErrListen) {
		return false
	}

	s.log.Warn("Retrying create with fresh ports after a server or the agent could not listen", "instance", instanceID, "err", err)

	return true
}

func (s *Service) createServer(req micro.Request) {
	s.log.Info("Handling create server request")
	retryCreate(req, s.createServerAttempt)
}

func (s *Service) createServerAttempt(req micro.Request, final bool) (retry bool) {
	creq := api.CreateServerRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	s.log.Debug("Create server", "request", creq)

	if err := validateSnippetKeys(creq.Snippets); err != nil {
		req.Error("007", err.Error(), nil)
		return
	}

	if err := validateJetStreamSnippet(creq.Snippets, creq.JetStream); err != nil {
		req.Error("011", err.Error(), nil)
		return
	}

	if err := validateTLSRequest(creq.TLS, creq.Snippets, creq.Template); err != nil {
		req.Error("007", err.Error(), nil)
		return
	}

	mainTemplate := serverConfigTemplate
	if creq.Template != "" {
		mainTemplate = creq.Template
	}

	// Trace implies the proxy; Proxy alone fronts the server and stores nothing.
	proxied := creq.Trace || creq.Proxy

	// The capture proxy is a plaintext forwarder, so it cannot front a TLS client port.
	if proxied && creq.TLS != nil {
		req.Error("013", "trace capture does not support TLS instances", nil)
		return
	}

	if proxied && s.agents == nil {
		req.Error("014", "trace capture is not configured on this service", nil)
		return
	}

	if proxied && !s.agentDeclares(agent.CapabilityCapture) {
		req.Error("018", fmt.Sprintf("the %q agent does not declare the %q capability", s.agentName, agent.CapabilityCapture), nil)
		return
	}

	if creq.Toxics {
		code, msg := s.toxicsRefusal("server", creq.Template)
		if code != "" {
			req.Error(code, msg, nil)
			return
		}
	}

	inst := s.newInstance("server", creq.Description)

	spec := instanceSpec{
		Kind:          "server",
		Servers:       1,
		JetStream:     creq.JetStream,
		Description:   creq.Description,
		Snippets:      creq.Snippets,
		MainTemplate:  mainTemplate,
		AdvertiseHost: effectiveAdvertiseHost(s.advertiseHost, creq.TLS),
		Proxy:         proxied,
		StoreCaptures: creq.Trace,
		Toxics:        creq.Toxics,
	}
	setSpecTLS(&spec, creq.TLS)

	return s.createFromSpec(req, final, inst, spec)
}

// setSpecTLS copies a create request's TLS options into spec. A nil opts leaves
// generated TLS off. An empty mode means mutual TLS.
func setSpecTLS(spec *instanceSpec, opts *api.TLSOptions) {
	if opts == nil {
		return
	}

	spec.TLS = true
	spec.TLSMutual = opts.Mode != api.TLSModeServer
	spec.TLSSANs = opts.SANs
	spec.TLSHandshakeFirst = opts.HandshakeFirst
	spec.TLSTimeout = opts.Timeout
}

// createFromSpec runs the part of a create attempt every kind of instance
// shares, once the request is validated and inst is registered. It asks the
// backend for a placement, plans inst from spec against it, starts the agent
// when the instance needs one, provisions the plan, fronts the plan's proxy
// node with the agent, publishes the nodes and answers req with the create
// response.
//
// It answers req itself, unless a node or the agent could not listen and final
// is false: it then stops the agent, tears down everything the attempt started
// and returns true to have the create run again, which plans the instance again
// from the spec with a fresh placement.
func (s *Service) createFromSpec(req micro.Request, final bool, inst *instance, spec instanceSpec) (retry bool) {
	if spec.TLS {
		s.log.Info("Issuing TLS server cert", "instance", inst.ID, "sans", effectiveSANs(spec.TLSSANs, spec.AdvertiseHost))
	}

	ctx := context.Background()

	place, err := s.backend.Placement(ctx, inst.ID)
	if err != nil {
		s.dropInstance(inst)
		code, msg := backendErrorResponse(err)
		req.Error(code, msg, nil)
		return false
	}

	plan, err := planInstance(spec, inst.ID, place, s.backend.DefaultVersion())
	if err != nil {
		s.dropInstance(inst)
		req.Error(planErrorCode(err), err.Error(), nil)
		return false
	}

	// The instance needs an agent when its clients or links go through a
	// proxy. The agent starts before the servers, so its listeners are up
	// before they dial them, and the ports of its route and gateway proxies
	// are handed to it just before it starts. Every failure from here on
	// stops it.
	var netAgent agent.Agent
	if spec.Proxy || spec.Toxics {
		var names []string
		for _, p := range plan.proxies {
			names = append(names, p.Name)
		}
		s.releaseProxyHolds(inst.ID, names)

		netAgent, err = s.startAgent(inst, spec.AdvertiseHost, spec.StoreCaptures, plan)
		if err != nil {
			s.dropInstance(inst)
			if s.retryAfter(err, final, inst.ID) {
				return true
			}
			req.Error("012", fmt.Sprintf("trace setup failed: %v", err), nil)
			return false
		}
	}

	started, err := s.backend.Provision(ctx, &plan.InstancePlan)
	if err != nil {
		s.stopAgent(inst, netAgent)
		s.dropInstance(inst)
		if s.retryAfter(err, final, inst.ID) {
			return true
		}
		code, msg := backendErrorResponse(err)
		req.Error(code, msg, nil)
		return false
	}

	resp := api.CreateResponse{
		ID:          inst.ID,
		Description: inst.Description,
		Kind:        inst.Kind,
		Servers:     []*api.ManagedServer{},
		TLS:         plan.TLS,
	}

	// The route and gateway links, in plan order. Each node's client link
	// follows once its live client address is known.
	for _, p := range plan.proxies {
		resp.Links = append(resp.Links, api.Link{Name: p.Name, Kind: p.Kind, From: p.From, To: p.To, Proxy: p.Listen.String(), Upstream: p.Upstream.String()})
	}

	// The nodes in start order. Once the plan is kept on the instance, an update
	// may replace entries of plan.nodes and plan.Nodes under s.mu, so this
	// function works from its own copy of the slice.
	nodes := slices.Clone(plan.nodes)

	// Every file is written, every node runs its config and resp holds the TLS
	// material, so the plan's contents can go before it is kept. The plan is
	// kept before any node is published, so an update of a published node
	// always finds it. A published node plan never changes; an update replaces
	// it.
	plan.dropContents()
	for _, n := range nodes {
		n.dropContents()
	}

	s.mu.Lock()
	inst.plan = plan
	s.mu.Unlock()

	servers := make([]*managedServer, 0, len(nodes))
	ready := make([]*events.NodeAction, 0, len(nodes))

	for i, n := range nodes {
		node := started[i]

		var client backend.Address
		for _, a := range node.Addresses() {
			if a.Name == "client" {
				client = a
				break
			}
		}

		// The wire view of the node's listener ports. It is a map of its own, so
		// the trace port added below never reaches the template data update
		// validates against.
		var ports map[string]int
		if len(n.TemplateData.Ports) > 0 {
			ports = maps.Clone(n.TemplateData.Ports)
		}

		// With toxics, clients reach the node through its client proxy, whose
		// port is handed to the agent here, just before the agent call that
		// binds it. The real client port stays reachable as the link's
		// upstream.
		clientPort := client.External.Port
		if spec.Toxics {
			name := clientLinkName(n.Name)
			s.releaseProxyHolds(inst.ID, []string{name})

			proxy := plan.clientProxies[n.Name]
			clientPort = proxy.Port
			resp.Links = append(resp.Links, api.Link{Name: name, Kind: "client", To: n.Name, Proxy: n.TemplateData.ClientAdvertise, Upstream: client.Internal.String()})
		}

		ms := &managedServer{backendNode: node, instanceID: inst.ID, rootDir: filepath.Join(place.InstanceDir(), n.Dir), ports: ports, clientPort: clientPort, advertiseHost: spec.AdvertiseHost, node: n, toxics: spec.Toxics}

		// The agent of an instance with toxics is kept on its first node's
		// record, which teardown stops it from.
		if spec.Toxics && i == 0 {
			ms.agent = netAgent
		}

		// Front the node's client listener with the agent when the plan asks
		// for it, before the node is published so its trace port and agent are
		// set atomically. The live client address is fronted, since a custom
		// template may set its own client port.
		if n.Name == plan.ProxyNode {
			ep, terr := s.frontClient(netAgent, n.Name, client.Internal)
			if terr != nil {
				s.stopAgent(inst, netAgent)
				s.dropInstance(inst)
				if s.retryAfter(terr, final, inst.ID) {
					return true
				}
				req.Error("012", fmt.Sprintf("trace setup failed: %v", terr), nil)
				return false
			}
			ms.agent = netAgent
			ms.fronted = true
			if ports == nil {
				ports = map[string]int{}
			}
			ports["trace"] = ep.Port
			ms.ports = ports
		}

		servers = append(servers, ms)

		// A node a concurrent teardown has stopped fails its status, and shows
		// as not running.
		st, err := node.Status(ctx)
		serverName := n.Name
		if err == nil {
			serverName = st.ServerName
		}
		ready = append(ready, &events.NodeAction{Action: events.NodeReady, Node: n.Name, ServerName: serverName})
		resp.Servers = append(resp.Servers, &api.ManagedServer{
			Name:      n.Name,
			Port:      clientPort,
			Ports:     ports,
			Cluster:   n.Cluster,
			Advertise: spec.AdvertiseHost,
			Running:   err == nil && st.Running,
		})
	}

	// Publish every node in one step, and only while the instance is still
	// registered. A destroy or reset that removed it meanwhile never saw the
	// agent, so stop it first, then tear down what this create started since.
	// The agent may have created its scratch directory after that teardown
	// removed the instance directory, so remove the directory again.
	//
	// The event lock is held across publishing the nodes and recording them,
	// so a node handler or a teardown that finds them records its event after
	// created and ready.
	inst.events.Lock()
	s.mu.Lock()
	_, alive := s.instances[inst.ID]
	if alive {
		inst.Servers = servers
		inst.links = resp.Links
	}
	s.mu.Unlock()

	if alive {
		s.publishEvent(inst, &events.InstanceAction{Action: events.InstanceCreated, Kind: inst.Kind, Backend: s.backendName})
		for _, e := range ready {
			s.publishEvent(inst, e)
		}
		inst.announced = true
	}
	inst.events.Unlock()

	if !alive {
		s.stopAgent(inst, netAgent)
		s.dropInstance(inst)
		if !s.preserve {
			err := os.RemoveAll(inst.RootDir)
			if err != nil {
				s.log.Error("Failed to remove instance dir", "instance", inst.ID, "err", err)
			}
		}
		req.Error("404", "Instance not found", nil)
		return false
	}

	req.RespondJSON(resp)

	return false
}

// planErrorCode returns the response code a create answers a planning failure
// with.
func planErrorCode(err error) string {
	var perr *planError
	if !errors.As(err, &perr) {
		return "002"
	}

	switch perr.Kind {
	case planErrTLSSetup, planErrTLSSnippet:
		return "008"
	case planErrPort:
		return "006"
	case planErrRouteTLS:
		return "019"
	default:
		return "002"
	}
}

// backendErrorResponse returns the response code and message a create answers
// a backend failure with: those an in-process failure carries, or 003 and the
// error's message for any other.
func backendErrorResponse(err error) (string, string) {
	var berr *inProcessError
	if errors.As(err, &berr) {
		return berr.Code, berr.Message
	}

	return "003", err.Error()
}

// revisionErrorMessage returns the message an update answers a failure to
// render a node's next revision with, under code 009: a snippet failure as a
// snippet render failure, anything else as a config render failure.
func revisionErrorMessage(err error) string {
	var perr *planError
	if !errors.As(err, &perr) {
		return fmt.Sprintf("config render failed: %v", err)
	}

	if perr.Kind == planErrSnippet {
		return fmt.Sprintf("snippet render failed: %v", perr.Err)
	}

	return fmt.Sprintf("config render failed: %v", perr.Err)
}

// traceSetupTimeout limits an agent's start, and its fronting of one server, each
// of which may include first-use work such as creating the store captures are
// written to.
const traceSetupTimeout = 30 * time.Second

// agentDeclares reports whether the selected agent declares c. It is false when
// no agent is selected.
func (s *Service) agentDeclares(c agent.Capability) bool {
	if s.agents == nil {
		return false
	}

	return slices.Contains(s.agents.Capabilities(), c)
}

// startAgent asks the agent provider to start the agent of inst, before its
// servers start, and returns the agent.
//
// advertiseHost becomes the agent's listen host, so the advertised trace URL
// matches the node's client_advertise. It must name a local interface or the
// agent fails to listen. Empty binds all interfaces (the no-advertise default).
// store is passed to the agent as Spec.Store: true keeps every connection's
// capture, false forwards and shapes without keeping any. The proxies of plan,
// which has some only when created with toxics, are passed as Spec.Links and
// Spec.ClientProxies. The caller must tear the instance down on failure.
func (s *Service) startAgent(inst *instance, advertiseHost string, store bool, plan *instancePlan) (agent.Agent, error) {
	if s.agents == nil {
		return nil, fmt.Errorf("trace capture is not configured on this service")
	}

	ctx, cancel := context.WithTimeout(context.Background(), traceSetupTimeout)
	defer cancel()

	return s.agents.Start(ctx, agent.Spec{
		InstanceID:    inst.ID,
		ListenHost:    advertiseHost,
		Store:         store,
		Links:         slices.Clone(plan.proxies),
		ClientProxies: maps.Clone(plan.clientProxies),
	})
}

// proxyHolder is a backend whose placement holds the port of each proxy until
// the service hands it to the agent. The in-process backend is one.
type proxyHolder interface {
	// releaseProxies closes what holds the ports of the named proxies of the
	// instance instanceID. The ports stay reserved until Teardown.
	releaseProxies(instanceID string, names []string)
}

// releaseProxyHolds hands the ports of the named proxies of the instance
// instanceID to its agent, just before the agent binds them, when the backend
// holds them.
func (s *Service) releaseProxyHolds(instanceID string, names []string) {
	holder, ok := s.backend.(proxyHolder)
	if !ok {
		return
	}

	holder.releaseProxies(instanceID, names)
}

// frontClient asks a, a running agent, to front the client listener of the
// managed server serverName, which it dials at client, and returns the endpoint
// clients reach it on. It is called only for an instance whose create asked for
// the proxy, which the create refuses unless the agent declares capture, so an
// agent that does not implement agent.ClientEdge is a fault in the agent. The
// caller must stop the agent and tear the instance down on failure.
func (s *Service) frontClient(a agent.Agent, serverName string, client backend.Endpoint) (backend.Endpoint, error) {
	edge, ok := a.(agent.ClientEdge)
	if !ok {
		return backend.Endpoint{}, fmt.Errorf("the %q agent declares the %q capability but does not implement agent.ClientEdge", s.agentName, agent.CapabilityCapture)
	}

	ctx, cancel := context.WithTimeout(context.Background(), traceSetupTimeout)
	defer cancel()

	ep, err := edge.Front(ctx, agent.Front{ServerName: serverName, Client: client})
	if err != nil {
		return backend.Endpoint{}, err
	}

	s.log.Info("Trace proxy listening", "server", serverName, "address", ep.String())
	return ep, nil
}

// stopAgent stops a, the agent a create of inst started, when the create fails
// before the instance is published. A failure is logged. A nil a is no agent.
func (s *Service) stopAgent(inst *instance, a agent.Agent) {
	if a == nil {
		return
	}

	err := a.Stop(context.Background())
	if err != nil {
		s.log.Error("Failed to stop agent", "instance", inst.ID, "err", err)
	}
}

func (s *Service) createCluster(req micro.Request) {
	s.log.Info("Handling create cluster request")
	start := time.Now()
	defer func() { s.log.Info("Handled create cluster request", "duration", time.Since(start)) }()

	retryCreate(req, s.createClusterAttempt)
}

func (s *Service) createClusterAttempt(req micro.Request, final bool) (retry bool) {
	creq := api.CreateClusterRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	if creq.Servers < 2 {
		req.Error("002", "Invalid request: servers should be at least 2", nil)
		return
	}

	if creq.Servers > maxServersPerCluster {
		req.Error("002", fmt.Sprintf("Invalid request: servers should be at most %d", maxServersPerCluster), nil)
		return
	}

	if err := validateSnippetKeys(creq.Snippets); err != nil {
		req.Error("007", err.Error(), nil)
		return
	}

	if err := validateJetStreamSnippet(creq.Snippets, creq.JetStream); err != nil {
		req.Error("011", err.Error(), nil)
		return
	}

	if err := validateTLSRequest(creq.TLS, creq.Snippets, creq.Template); err != nil {
		req.Error("007", err.Error(), nil)
		return
	}

	mainTemplate := serverConfigTemplate
	if creq.Template != "" {
		mainTemplate = creq.Template
	}

	// Trace implies the proxy; Proxy alone fronts the first node and stores nothing.
	proxied := creq.Trace || creq.Proxy

	if proxied && creq.TLS != nil {
		req.Error("013", "trace capture does not support TLS instances", nil)
		return
	}

	if proxied && s.agents == nil {
		req.Error("014", "trace capture is not configured on this service", nil)
		return
	}

	if proxied && !s.agentDeclares(agent.CapabilityCapture) {
		req.Error("018", fmt.Sprintf("the %q agent does not declare the %q capability", s.agentName, agent.CapabilityCapture), nil)
		return
	}

	if creq.Toxics {
		code, msg := s.toxicsRefusal("cluster", creq.Template)
		if code != "" {
			req.Error(code, msg, nil)
			return
		}
	}

	inst := s.newInstance("cluster", creq.Description)
	inst.Cluster = "C_" + shortID(inst.ID)

	spec := instanceSpec{
		Kind:          "cluster",
		Servers:       creq.Servers,
		JetStream:     creq.JetStream,
		Description:   creq.Description,
		Snippets:      creq.Snippets,
		MainTemplate:  mainTemplate,
		AdvertiseHost: effectiveAdvertiseHost(s.advertiseHost, creq.TLS),
		Proxy:         proxied,
		StoreCaptures: creq.Trace,
		Toxics:        creq.Toxics,
	}
	setSpecTLS(&spec, creq.TLS)

	return s.createFromSpec(req, final, inst, spec)
}

func (s *Service) createSuperCluster(req micro.Request) {
	s.log.Info("Handling create super cluster request")
	retryCreate(req, s.createSuperClusterAttempt)
}

func (s *Service) createSuperClusterAttempt(req micro.Request, final bool) (retry bool) {
	creq := api.CreateSuperClusterRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	if creq.Servers < 2 {
		req.Error("002", "Invalid request: servers should be at least 2", nil)
		return
	}

	if creq.Servers > maxServersPerCluster {
		req.Error("002", fmt.Sprintf("Invalid request: servers should be at most %d", maxServersPerCluster), nil)
		return
	}

	if creq.Clusters < 2 {
		req.Error("004", "Invalid request: clusters should be at least 2", nil)
		return
	}

	if creq.Clusters > maxClusters {
		req.Error("004", fmt.Sprintf("Invalid request: clusters should be at most %d", maxClusters), nil)
		return
	}

	if err := validateSnippetKeys(creq.Snippets); err != nil {
		req.Error("007", err.Error(), nil)
		return
	}

	if err := validateJetStreamSnippet(creq.Snippets, creq.JetStream); err != nil {
		req.Error("011", err.Error(), nil)
		return
	}

	if err := validateTLSRequest(creq.TLS, creq.Snippets, creq.Template); err != nil {
		req.Error("007", err.Error(), nil)
		return
	}

	mainTemplate := serverConfigTemplate
	if creq.Template != "" {
		mainTemplate = creq.Template
	}

	// Trace implies the proxy; Proxy alone fronts the first node and stores nothing.
	proxied := creq.Trace || creq.Proxy

	if proxied && creq.TLS != nil {
		req.Error("013", "trace capture does not support TLS instances", nil)
		return
	}

	if proxied && s.agents == nil {
		req.Error("014", "trace capture is not configured on this service", nil)
		return
	}

	if proxied && !s.agentDeclares(agent.CapabilityCapture) {
		req.Error("018", fmt.Sprintf("the %q agent does not declare the %q capability", s.agentName, agent.CapabilityCapture), nil)
		return
	}

	if creq.Toxics {
		code, msg := s.toxicsRefusal("super-cluster", creq.Template)
		if code != "" {
			req.Error(code, msg, nil)
			return
		}
	}

	inst := s.newInstance("super-cluster", creq.Description)

	spec := instanceSpec{
		Kind:          "super-cluster",
		Servers:       creq.Servers,
		Clusters:      creq.Clusters,
		JetStream:     creq.JetStream,
		Description:   creq.Description,
		Snippets:      creq.Snippets,
		MainTemplate:  mainTemplate,
		AdvertiseHost: effectiveAdvertiseHost(s.advertiseHost, creq.TLS),
		Proxy:         proxied,
		StoreCaptures: creq.Trace,
		Toxics:        creq.Toxics,
	}
	setSpecTLS(&spec, creq.TLS)

	return s.createFromSpec(req, final, inst, spec)
}

func (s *Service) reset(req micro.Request) {
	s.log.Info("Handling reset request")

	err := s.Reset()
	if err != nil {
		req.Error("004", fmt.Sprintf("Server reset failed: %v", err), nil)
		return
	}

	req.RespondJSON(api.ResetResponse{Shutdown: true})
}

func (s *Service) stopServer(req micro.Request) {
	s.log.Info("Handling stop request")

	creq := api.StopServerRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	if creq.Name == "" {
		req.Error("002", "Invalid request: server name is required", nil)
		return
	}

	ms := s.findServerByName(creq.Name)
	if ms == nil {
		req.Error("001", "Server not found", nil)
		return
	}

	// Hold cfgMu so a concurrent start cannot start the node while it is checked
	// and stopped here: startServer holds cfgMu across its start.
	ms.cfgMu.Lock()
	defer ms.cfgMu.Unlock()

	ctx := context.Background()

	// A node whose instance a concurrent destroy tore down fails its status,
	// and is not running.
	st, err := ms.backendNode.Status(ctx)
	if err != nil || !st.Running {
		req.Error("003", "Server not running", nil)
		return
	}

	err = ms.backendNode.Stop(ctx)
	if err != nil {
		req.Error("004", "Server did not shut down", nil)
		return
	}

	st, err = ms.backendNode.Status(ctx)
	if err == nil && st.Running {
		req.Error("004", "Server did not shut down", nil)
		return
	}

	s.recordNodeEvent(ms, events.NodeStopped, creq.Name)

	req.RespondJSON(api.StopServerResponse{Shutdown: true})
}

func (s *Service) startServer(req micro.Request) {
	s.log.Info("Handling start request")

	creq := api.StartServerRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	if creq.Name == "" {
		req.Error("002", "Invalid request: server name is required", nil)
		return
	}

	ms := s.findServerByName(creq.Name)
	if ms == nil {
		req.Error("001", "Server not found", nil)
		return
	}

	// Hold cfgMu across the whole start: it keeps a concurrent start from
	// starting the node underneath the running check, keeps two callers from
	// both passing that check and starting the same server twice, and keeps a
	// concurrent update from being mid-write when the start reads the config
	// path.
	ms.cfgMu.Lock()
	defer ms.cfgMu.Unlock()

	ctx := context.Background()
	node := ms.backendNode

	st, err := node.Status(ctx)
	if err == nil && st.Running {
		req.Error("003", "Server already running", nil)
		return
	}

	err = node.Start(ctx)

	// A concurrent destroy or reset may have removed the instance while this
	// node was starting. A start its teardown refused fails, and one that
	// finished first is stopped here, rather than left holding its port for the
	// life of the process.
	s.mu.Lock()
	_, alive := s.instances[ms.instanceID]
	s.mu.Unlock()

	if !alive {
		if err == nil {
			serr := node.Stop(ctx)
			if serr != nil {
				s.log.Warn("Could not stop server of a destroyed instance", "server", creq.Name, "err", serr)
			}
		}
		req.Error("404", "Instance destroyed during start", nil)
		return
	}

	if err != nil {
		var berr *inProcessError
		if errors.As(err, &berr) {
			req.Error(berr.Code, berr.Message, nil)
			return
		}
		req.Error("005", fmt.Sprintf("Server restart failed: %v", err), nil)
		return
	}

	s.recordNodeEvent(ms, events.NodeStarted, creq.Name)

	req.RespondJSON(api.StartServerResponse{Started: true})
}

// stopInstance shuts down every running server in an instance while leaving its
// config and storage on disk, so startInstance can revive it. It is best-effort
// and idempotent: nodes already stopped are left as-is. Each node is handled
// under its cfgMu, serializing against the per-server start/stop/update/reload
// handlers and startInstance.
func (s *Service) stopInstance(req micro.Request) {
	s.log.Info("Handling stop instance request")

	creq := api.StopInstanceRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	if creq.InstanceID == "" {
		req.Error("002", "Invalid request: instance_id is required", nil)
		return
	}

	s.mu.Lock()
	inst, ok := s.instances[creq.InstanceID]
	var servers []*managedServer
	if ok {
		servers = slices.Clone(inst.Servers)
	}
	s.mu.Unlock()

	if !ok {
		req.Error("404", "Instance not found", nil)
		return
	}

	ctx := context.Background()

	results := make([]api.ServerStateResult, 0, len(servers))
	for _, ms := range servers {
		node := ms.backendNode

		ms.cfgMu.Lock()
		// stopped reports whether this request stopped the node, rather than
		// finding it stopped already.
		stopped := false
		st, err := node.Status(ctx)
		if err == nil && st.Running {
			err = node.Stop(ctx)
			if err != nil {
				s.log.Warn("Could not stop server", "server", node.Name(), "err", err)
			}
			stopped = err == nil
		}
		// The plan name stands in when a destroy tore the node down meanwhile.
		res := api.ServerStateResult{Name: node.Name()}
		st, err = node.Status(ctx)
		if err == nil {
			res.Name = st.ServerName
			res.Running = st.Running
		}
		if stopped && err == nil && !res.Running {
			s.recordNodeEvent(ms, events.NodeStopped, res.Name)
		}
		ms.cfgMu.Unlock()
		results = append(results, res)
	}

	req.RespondJSON(api.StopInstanceResponse{Servers: results})
}

// startInstance revives a stopped instance, restarting each non-running server
// from its persisted config. It is best-effort and idempotent: already-running
// nodes are left alone, and a node whose port was taken in the stop/start gap
// fails individually with its error reported in the per-node result rather than
// aborting the whole instance.
func (s *Service) startInstance(req micro.Request) {
	s.log.Info("Handling start instance request")

	creq := api.StartInstanceRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	if creq.InstanceID == "" {
		req.Error("002", "Invalid request: instance_id is required", nil)
		return
	}

	s.mu.Lock()
	inst, ok := s.instances[creq.InstanceID]
	var servers []*managedServer
	if ok {
		servers = slices.Clone(inst.Servers)
	}
	s.mu.Unlock()

	if !ok {
		req.Error("404", "Instance not found", nil)
		return
	}

	ctx := context.Background()

	results := make([]api.ServerStateResult, 0, len(servers))
	for _, ms := range servers {
		node := ms.backendNode
		// The plan name stands in when a destroy tore the node down meanwhile.
		res := api.ServerStateResult{Name: node.Name()}

		ms.cfgMu.Lock()
		st, err := node.Status(ctx)
		if err == nil {
			res.Name = st.ServerName
		}

		if err == nil && st.Running {
			res.Running = true
			ms.cfgMu.Unlock()
			results = append(results, res)
			continue
		}

		err = node.Start(ctx)

		// A concurrent destroy may have removed the instance while this node was
		// starting. A start its teardown refused fails, and one that finished
		// first is stopped here rather than leaked, and the request stops.
		s.mu.Lock()
		_, alive := s.instances[creq.InstanceID]
		s.mu.Unlock()

		if !alive {
			if err == nil {
				serr := node.Stop(ctx)
				if serr != nil {
					s.log.Warn("Could not stop server of a destroyed instance", "server", node.Name(), "err", serr)
				}
			}
			ms.cfgMu.Unlock()
			req.Error("404", "Instance destroyed during start", nil)
			return
		}

		// A node with no config to start from reports the cause its error
		// carries.
		if errors.Is(err, errNoPersistedConfig) {
			ms.cfgMu.Unlock()
			res.Error = errNoPersistedConfig.Error()
			results = append(results, res)
			continue
		}

		if err != nil {
			ms.cfgMu.Unlock()
			res.Error = err.Error()
			results = append(results, res)
			continue
		}

		// The started node's state is read and recorded under cfgMu, so its
		// event is ordered against the node's other changes.
		st, err = node.Status(ctx)
		if err == nil {
			res.Name = st.ServerName
			res.Running = st.Running
		}
		if err == nil && res.Running {
			s.recordNodeEvent(ms, events.NodeStarted, res.Name)
		}
		ms.cfgMu.Unlock()
		results = append(results, res)
	}

	req.RespondJSON(api.StartInstanceResponse{Servers: results})
}

func (s *Service) updateServer(req micro.Request) {
	s.log.Info("Handling update server request")

	creq := api.UpdateServerRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	if creq.Name == "" {
		req.Error("001", "Invalid request: server name is required", nil)
		return
	}

	ms := s.findServerByName(creq.Name)
	if ms == nil {
		req.Error("001", "Server not found", nil)
		return
	}

	ms.cfgMu.Lock()
	defer ms.cfgMu.Unlock()

	if err := validateSnippetKeys(creq.Snippets); err != nil {
		req.Error("007", err.Error(), nil)
		return
	}

	node := ms.node

	if err := validateTLSSnippetRefs(creq.Snippets, node.TemplateData.TLS != nil); err != nil {
		req.Error("007", err.Error(), nil)
		return
	}

	if err := validateJetStreamSnippet(creq.Snippets, node.TemplateData.JetStream); err != nil {
		req.Error("011", err.Error(), nil)
		return
	}

	// The template data's Ports is the listener set the running config was
	// rendered with. ms.ports is the wire view, which also carries the capture
	// proxy's port for a traced server; validating against that would reject
	// every update of a traced server, since no snippet can ever produce a
	// "trace" listener.
	if err := validateListenerPortSet(creq.Snippets, node.TemplateData.Ports); err != nil {
		req.Error("008", err.Error(), nil)
		return
	}

	if creq.TLSTimeout != nil && node.TLS == nil {
		req.Error("007", "server was not created with WithGeneratedTLS; cannot set tls_timeout", nil)
		return
	}

	// The revision renders the proxies' addresses from the node's template
	// data, so a custom template must reference them as it must on create.
	if ms.toxics {
		err := validateToxicsTemplate(node.TemplateData.Kind, creq.Template)
		if err != nil {
			req.Error("007", err.Error(), nil)
			return
		}
	}

	mainTmpl := serverConfigTemplate
	if creq.Template != "" {
		mainTmpl = creq.Template
	}

	// Render the whole next revision before anything is written, so a failed
	// render writes nothing.
	next, err := planRevision(node, creq.Snippets, mainTmpl, creq.TLSTimeout)
	if err != nil {
		req.Error("009", revisionErrorMessage(err), nil)
		return
	}

	// The proxies of toxics read a plain CONNECT, so the revision must not
	// enable TLS on routes or gateways either.
	if ms.toxics {
		err = checkRouteTLS(next)
		if err != nil {
			req.Error("019", err.Error(), nil)
			return
		}
	}

	err = ms.backendNode.Apply(context.Background(), &next.NodePlan)
	if err != nil {
		var berr *inProcessError
		if errors.As(err, &berr) {
			req.Error(berr.Code, berr.Message, nil)
			return
		}
		req.Error("009", err.Error(), nil)
		return
	}

	// The node has the revision: drop its contents and swap it in for the
	// node's plan, here and in the instance's plan. The old node plan is left
	// as it was.
	next.dropContents()
	ms.node = next

	s.mu.Lock()
	inst := s.instances[ms.instanceID]
	if inst != nil && inst.plan != nil {
		i := slices.Index(inst.plan.nodes, node)
		if i >= 0 {
			inst.plan.nodes[i] = next
			inst.plan.Nodes[i] = &next.NodePlan
		}
	}
	s.mu.Unlock()

	s.recordNodeEvent(ms, events.NodeUpdated, creq.Name)

	req.RespondJSON(api.UpdateServerResponse{Updated: true})
}

func (s *Service) reloadServer(req micro.Request) {
	s.log.Info("Handling reload server request")

	creq := api.ReloadServerRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	if creq.Name == "" {
		req.Error("001", "Invalid request: server name is required", nil)
		return
	}

	ms := s.findServerByName(creq.Name)
	if ms == nil {
		req.Error("001", "Server not found", nil)
		return
	}

	// Hold cfgMu across both the running check and the reload: it keeps a
	// concurrent start from starting the node underneath them, keeps the server
	// from stopping between the two, and stops the reload (which re-reads the
	// config path) observing a torn file mid-write from a concurrent update.
	ms.cfgMu.Lock()
	defer ms.cfgMu.Unlock()

	ctx := context.Background()

	st, err := ms.backendNode.Status(ctx)
	if err != nil || !st.Running {
		req.Error("002", "Server not running", nil)
		return
	}

	err = ms.backendNode.Reload(ctx)
	if err != nil {
		req.Error("010", fmt.Sprintf("reload failed: %v", err), nil)
		return
	}

	s.recordNodeEvent(ms, events.NodeReloaded, creq.Name)

	req.RespondJSON(api.ReloadServerResponse{Reloaded: true})
}

func (s *Service) status(req micro.Request) {
	creq := api.StatusRequest{}
	// Empty body is allowed and means "all instances".
	if len(req.Data()) > 0 {
		if err := json.Unmarshal(req.Data(), &creq); err != nil {
			req.Error("001", "Invalid request", nil)
			return
		}
	}

	s.mu.Lock()
	var picked []*instance
	if creq.InstanceID != "" {
		if inst, ok := s.instances[creq.InstanceID]; ok {
			picked = []*instance{inst}
		}
	} else {
		picked = slices.Collect(maps.Values(s.instances))
	}
	snapshots := make([]instanceSnapshot, 0, len(picked))
	for _, inst := range picked {
		snapshots = append(snapshots, snapshotInstance(inst))
	}
	s.mu.Unlock()

	// Read each node's status with the lock released. A node that fails it
	// keeps the plan's name and cluster from the snapshot and shows as not
	// running.
	ctx := context.Background()
	for i := range snapshots {
		for j := range snapshots[i].servers {
			ss := &snapshots[i].servers[j]

			st, err := ss.node.Status(ctx)
			if err != nil {
				s.log.Warn("Could not read server status", "instance", snapshots[i].id, "server", ss.name, "err", err)
				continue
			}

			ss.name = st.ServerName
			ss.cluster = st.Cluster
			ss.running = st.Running
			ss.clusterPort = st.ClusterPort
			ss.version = st.Version
		}
	}

	// Leave out an instance a destroy or reset removed while its nodes were read.
	s.mu.Lock()
	snapshots = slices.DeleteFunc(snapshots, func(snap instanceSnapshot) bool {
		_, ok := s.instances[snap.id]
		return !ok
	})
	s.mu.Unlock()

	if creq.InstanceID != "" && len(snapshots) == 0 {
		req.Error("404", "Instance not found", nil)
		return
	}

	// One backend runs every instance, so each reports its name and the
	// capabilities it declares, in its order.
	var capabilities []string
	for _, c := range s.backend.Capabilities() {
		capabilities = append(capabilities, string(c))
	}

	// Likewise the agent, when one is selected.
	var agentCapabilities []string
	if s.agents != nil {
		for _, c := range s.agents.Capabilities() {
			agentCapabilities = append(agentCapabilities, string(c))
		}
	}

	resp := api.StatusResponse{Instances: make([]api.InstanceStatus, 0, len(snapshots))}
	for _, snap := range snapshots {
		ist := api.InstanceStatus{
			ID:                snap.id,
			Description:       snap.description,
			Kind:              snap.kind,
			Servers:           make([]api.ManagedServer, 0, len(snap.servers)),
			Backend:           s.backendName,
			Capabilities:      capabilities,
			Agent:             s.agentName,
			AgentCapabilities: agentCapabilities,
			Links:             snap.links,
			EventsSeq:         snap.eventsSeq,
		}
		for _, ms := range snap.servers {
			// Port is the client port; the route/cluster port is exposed under
			// Ports["cluster"]. The map is already a private clone.
			ports := ms.ports
			if ms.clusterPort != 0 {
				if ports == nil {
					ports = map[string]int{}
				}
				ports["cluster"] = ms.clusterPort
			}
			ist.Servers = append(ist.Servers, api.ManagedServer{
				Name:      ms.name,
				Cluster:   ms.cluster,
				Port:      ms.clientPort,
				Ports:     ports,
				Advertise: ms.advertiseHost,
				Running:   ms.running,
				Version:   ms.version,
			})
		}
		ist.ShapingSets = s.shapingSetIDs(snap.id, snap.agent)
		resp.Instances = append(resp.Instances, ist)
	}

	req.RespondJSON(resp)
}

// shapingSetIDs returns the ids of the shaping sets applied to a, in the order
// the agent reports them. A nil agent or one without shaping has none. On any
// other error it logs the error and returns none, so status never fails on it.
func (s *Service) shapingSetIDs(instanceID string, a agent.Agent) []string {
	if a == nil {
		return nil
	}

	ids, err := a.ShapingSets(context.Background())
	if errors.Is(err, agent.ErrUnsupported) {
		return nil
	}
	if err != nil {
		s.log.Warn("Could not read shaping sets for status", "instance", instanceID, "err", err)
		return nil
	}

	return ids
}

func (s *Service) destroy(req micro.Request) {
	s.log.Info("Handling destroy request")
	start := time.Now()
	defer func() { s.log.Info("Handled destroy request", "duration", time.Since(start)) }()

	creq := api.DestroyRequest{}
	if err := json.Unmarshal(req.Data(), &creq); err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}
	if creq.InstanceID == "" {
		req.Error("002", "Invalid request: instance_id is required", nil)
		return
	}

	s.mu.Lock()
	inst, ok := s.instances[creq.InstanceID]
	if ok {
		delete(s.instances, creq.InstanceID)
	}
	s.mu.Unlock()

	if !ok {
		req.Error("404", "Instance not found", nil)
		return
	}

	s.tearDownInstance(inst, events.ReasonDestroy)
	req.RespondJSON(api.DestroyResponse{Destroyed: true})
}

func (s *Service) list(req micro.Request) {
	s.mu.Lock()
	resp := api.ListResponse{Instances: make([]api.InstanceSummary, 0, len(s.instances))}
	for _, inst := range s.instances {
		resp.Instances = append(resp.Instances, api.InstanceSummary{
			ID:          inst.ID,
			Description: inst.Description,
			Kind:        inst.Kind,
			Cluster:     inst.Cluster,
			Servers:     len(inst.Servers),
			Created:     inst.Created,
		})
	}
	s.mu.Unlock()

	req.RespondJSON(resp)
}

// agentFor finds the agent fronting an instance. It answers the request itself
// on any failure and returns nil, so a handler returns when it gets nil back.
func (s *Service) agentFor(req micro.Request, instanceID string) agent.Agent {
	if instanceID == "" {
		req.Error("002", "Invalid request: instance_id is required", nil)
		return nil
	}

	s.mu.Lock()
	inst, ok := s.instances[instanceID]
	var servers []*managedServer
	if ok {
		servers = slices.Clone(inst.Servers)
	}
	s.mu.Unlock()

	if !ok {
		req.Error("404", "Instance not found", nil)
		return nil
	}

	a := frontingAgentOf(servers)
	if a == nil {
		req.Error("015", "Instance has no trace proxy", nil)
		return nil
	}

	return a
}

// answerShapeError answers req with the error an agent's shaping call
// returned: 016 when the agent has no shaping, 017 with the error's text
// otherwise.
func answerShapeError(req micro.Request, err error) {
	if errors.Is(err, agent.ErrUnsupported) {
		req.Error("016", "Shaping not supported by the trace capturer", nil)
		return
	}

	req.Error("017", err.Error(), nil)
}

func (s *Service) shapeSet(req micro.Request) {
	s.log.Info("Handling shape set request")

	creq := api.ShapeSetRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	a := s.agentFor(req, creq.InstanceID)
	if a == nil {
		return
	}

	err = a.Shape(context.Background(), creq.Set)
	if err != nil {
		answerShapeError(req, err)
		return
	}

	req.RespondJSON(api.ShapeSetResponse{Set: creq.Set.ID})
}

func (s *Service) shapeClear(req micro.Request) {
	s.log.Info("Handling shape clear request")

	creq := api.ShapeClearRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	a := s.agentFor(req, creq.InstanceID)
	if a == nil {
		return
	}

	cleared, err := a.ClearShaping(context.Background(), creq.Set)
	if err != nil {
		answerShapeError(req, err)
		return
	}
	if cleared == nil {
		cleared = []string{}
	}

	req.RespondJSON(api.ShapeClearResponse{Cleared: cleared})
}

func (s *Service) shapeReport(req micro.Request) {
	s.log.Info("Handling shape report request")

	creq := api.ShapeReportRequest{}
	err := json.Unmarshal(req.Data(), &creq)
	if err != nil {
		req.Error("001", "Invalid request", nil)
		return
	}

	a := s.agentFor(req, creq.InstanceID)
	if a == nil {
		return
	}

	sets, err := a.ShapingReport(context.Background(), creq.Set)
	if err != nil {
		answerShapeError(req, err)
		return
	}
	if sets == nil {
		sets = []api.ShapingReport{}
	}

	req.RespondJSON(api.ShapeReportResponse{Sets: sets})
}

// instanceSnapshot is a lock-released view of an instance's metadata and the
// reportable state of its servers, taken so a status response can be built
// without holding s.mu.
type instanceSnapshot struct {
	id          string
	kind        string
	description string
	servers     []serverSnapshot
	// agent is the agent fronting the instance, or nil when it has none. The
	// status handler asks it for the applied shaping sets after the lock is
	// released.
	agent agent.Agent
	// eventsSeq is the instance's events_seq, read with the rest of the
	// instance before any node state, so the snapshot is no older than it.
	eventsSeq uint64
	// links are the instance's proxied links, nil without toxics.
	links []api.Link
}

// serverSnapshot is a managed server's reportable state. snapshotInstance reads
// what the service holds while the caller holds s.mu; the status handler fills
// in what node.Status reports once the lock is released.
type serverSnapshot struct {
	name          string
	cluster       string
	clientPort    int
	ports         map[string]int
	advertiseHost string
	running       bool
	clusterPort   int    // 0 when the server has no cluster listener
	version       string // empty when node.Status failed
	node          backend.Node
}

// snapshotInstance reads an instance and its servers into plain values, with
// each server's name and cluster from the instance's plan until its node
// reports its own. The caller must hold s.mu.
func snapshotInstance(inst *instance) instanceSnapshot {
	srvs := make([]serverSnapshot, 0, len(inst.Servers))
	for _, ms := range inst.Servers {
		snap := serverSnapshot{
			name: ms.backendNode.Name(),
			// clientPort is captured at create time, so it survives a stop; the
			// route/cluster port is reported separately as clusterPort.
			clientPort:    ms.clientPort,
			ports:         maps.Clone(ms.ports),
			advertiseHost: ms.advertiseHost,
			node:          ms.backendNode,
		}
		if inst.plan != nil {
			for _, np := range inst.plan.Nodes {
				if np.Name == snap.name {
					snap.cluster = np.Cluster
				}
			}
		}
		srvs = append(srvs, snap)
	}

	return instanceSnapshot{
		id:          inst.ID,
		kind:        inst.Kind,
		description: inst.Description,
		servers:     srvs,
		agent:       frontingAgentOf(inst.Servers),
		eventsSeq:   inst.eventsSeq,
		links:       slices.Clone(inst.links),
	}
}

// agentOf returns the agent fronting one of servers, or nil when none is
// fronted. agent is set before a server joins its instance and never changes
// afterwards, so the caller may read the result without holding s.mu.
func agentOf(servers []*managedServer) agent.Agent {
	for _, ms := range servers {
		if ms.agent != nil {
			return ms.agent
		}
	}
	return nil
}

// frontingAgentOf returns the agent of the one of servers it fronts for trace
// or proxy, which the shaping calls act on, or nil when none is fronted. The
// agent an instance created with toxics alone keeps on its first server does
// not count. Like agent, fronted never changes once the server joins its
// instance.
func frontingAgentOf(servers []*managedServer) agent.Agent {
	for _, ms := range servers {
		if ms.fronted {
			return ms.agent
		}
	}
	return nil
}

// findServerByName scans every instance for a server whose runtime name matches,
// as its node's status reports it. The servers are read under s.mu and their
// nodes asked with the lock released; a node that fails its status does not
// match. Server names are made globally unique by the short-id prefix, so the
// first match is always the right one.
func (s *Service) findServerByName(name string) *managedServer {
	s.mu.Lock()
	var servers []*managedServer
	for _, inst := range s.instances {
		servers = append(servers, inst.Servers...)
	}
	s.mu.Unlock()

	ctx := context.Background()
	for _, ms := range servers {
		st, err := ms.backendNode.Status(ctx)
		if err != nil {
			continue
		}
		if st.ServerName == name {
			return ms
		}
	}
	return nil
}

// reservePort takes a free TCP port for one of inst's servers from the service's
// port range and returns the port plus the still-open listener holding it on the
// wildcard address (0.0.0.0). The port stays reserved until inst is torn down.
// The caller must close the listener immediately before binding the port for
// real (typically by passing it through runServerWithConfig).
func (s *Service) reservePort(inst *instance) (int, *net.TCPListener, error) {
	port, ln, err := s.ports.reserve()
	if err != nil {
		return 0, nil, err
	}

	s.mu.Lock()
	_, alive := s.instances[inst.ID]
	if alive {
		inst.ports = append(inst.ports, port)
	}
	s.mu.Unlock()

	// A concurrent destroy or reset has already torn inst down and will not
	// release this port, so do not keep it out of the range.
	if !alive {
		s.ports.release(port)
	}

	return port, ln, nil
}

func clientPortOf(srv *server.Server) (int, error) {
	u, err := url.Parse(srv.ClientURL())
	if err != nil {
		return 0, fmt.Errorf("could not parse client url: %w", err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		return 0, fmt.Errorf("could not parse client port: %w", err)
	}
	return port, nil
}

// beforeServerStart, when set, runs in runServerWithConfig after the server's
// port reservations are closed and before it starts, and in startFromConfig
// just before the server starts. Tests use it to take a port in that gap, or to
// stall a start. Nil outside tests.
var beforeServerStart func(opts *server.Options)

// runServerWithConfig writes the rendered NATS config to a file under rootDir,
// closes the held port-reservation listeners, and starts the server. The
// listeners must remain open through config write and server construction so
// that concurrent create calls cannot grab the same ports. A server that fails
// to start is shut down; the error wraps ErrServerListen when it could not bind
// one of its ports.
func runServerWithConfig(log *slog.Logger, config []byte, rootDir string, listeners []*net.TCPListener) (*server.Server, string, error) {
	tf, err := os.CreateTemp(rootDir, "*.cfg")
	if err != nil {
		closeListeners(listeners)
		return nil, "", err
	}
	configPath := tf.Name()
	_, err = tf.Write(config)
	tf.Close()
	if err != nil {
		closeListeners(listeners)
		return nil, "", err
	}

	opts, err := server.ProcessConfigFile(configPath)
	if err != nil {
		closeListeners(listeners)
		return nil, "", fmt.Errorf("could not process config file: %v: %w", configPath, err)
	}

	opts.NoSigs = true

	srv, err := server.NewServer(opts)
	if err != nil {
		closeListeners(listeners)
		return nil, "", err
	}

	guard := guardServerLogger(srv, opts, log)

	// Hand over the reserved ports to the server: close the listeners
	// immediately before Start() to minimize the window where a concurrent
	// create call could win the same port.
	closeListeners(listeners)

	if beforeServerStart != nil {
		beforeServerStart(opts)
	}

	log.Info("Starting server", "server", srv.Name(), "dir", rootDir)
	srv.Start()

	err = waitForStart(srv, guard)
	if err != nil {
		srv.Shutdown()
		srv.WaitForShutdown()
		return nil, "", fmt.Errorf("server %s failed to start: %w", srv.Name(), err)
	}

	log.Info("Started server", "server", srv.Name(), "url", srv.ClientURL())

	return srv, configPath, nil
}

// startFromConfig rebuilds and starts a server from a previously-written config
// file. Used by the in-process node's Start to bring a stopped server back
// online; the underlying *server.Server cannot be restarted in place after
// Shutdown. A server that fails to start is shut down and not retried: its
// ports are fixed in the config its peers know.
func startFromConfig(log *slog.Logger, configPath string) (*server.Server, error) {
	opts, err := server.ProcessConfigFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("could not process config file: %v: %w", configPath, err)
	}
	opts.NoSigs = true

	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}

	guard := guardServerLogger(srv, opts, log)

	if beforeServerStart != nil {
		beforeServerStart(opts)
	}

	log.Info("Restarting server", "server", srv.Name())
	srv.Start()

	err = waitForStart(srv, guard)
	if err != nil {
		srv.Shutdown()
		srv.WaitForShutdown()
		return nil, fmt.Errorf("server %s failed to restart: %w", srv.Name(), err)
	}

	log.Info("Restarted server", "server", srv.Name(), "url", srv.ClientURL())

	return srv, nil
}

// cloneTemplateData returns a shallow copy of td with a fresh empty
// Snippets map. Used by planRevision to render against a copy so an
// in-progress update doesn't mutate the node plan's template data; the
// node plan is only swapped for one holding the copy on success.
func cloneTemplateData(td *templateData) *templateData {
	c := *td
	c.Snippets = map[string]string{}
	return &c
}

func defaultTemplateData() *templateData {
	return &templateData{
		ClusterPort: -1,
		ClientPort:  -1,
		Snippets:    map[string]string{},
		Ports:       map[string]int{},
	}
}

// templateData is the env exposed to the main config template, to
// user-supplied snippets, and to full-template overrides.
type templateData struct {
	// Per-server.
	ServerName    string
	ShortID       string
	InstanceID    string
	ServerIndex   int
	ServerDir     string
	StoreDir      string
	LogFile       string
	Host          string
	AdvertiseHost string
	ClientPort    int
	JetStream     bool

	// Cluster.
	ClusterName  string
	ClusterIndex int
	ClusterPort  int
	Routes       []string
	ClusterSize  int

	// Super-cluster.
	Clusters    []string
	GatewayPort int
	Gateways    map[string][]string

	// Instance-wide.
	Description string
	Kind        string

	// Caller-declared listener ports, keyed by listener name (.Ports.<name>).
	Ports map[string]int

	// User-supplied snippet include paths, keyed by extension-point name.
	// Always non-nil so {{ if .Snippets.foo }} guards never index a nil map.
	Snippets map[string]string

	// TLSInclude is the per-server include path for the managed TLS snippet
	// generated when CreateRequest.TLS is set. Empty when TLS is not
	// requested. Custom templates that opt into generated TLS must emit
	// `{{ if .TLSInclude }}include "{{ .TLSInclude }}"{{ end }}`.
	TLSInclude string

	// TLS exposes the generated cert file paths to snippets as
	// .TLS.CAFile/.CertFile/.KeyFile, so a listener snippet can build its own
	// tls{} block (e.g. websocket wss). Nil unless generated TLS is requested.
	TLS *templateTLS

	// ClientAdvertise is the host:port of the server's client proxy, which an
	// instance created with toxics renders as client_advertise. Empty without
	// toxics.
	ClientAdvertise string

	// ClusterAdvertise and GatewayAdvertise are the host:port of the dead
	// address an instance created with toxics renders as the advertise of its
	// cluster and gateway blocks, so a peer that learns of a node by gossip
	// is refused rather than going around the proxies. Empty without toxics,
	// and for a block the instance does not have.
	ClusterAdvertise string
	GatewayAdvertise string
}
