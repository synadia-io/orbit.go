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

	"github.com/synadia-io/orbit.go/ntf/api"
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
server_name: {{ .ServerName }}
port: {{ .ClientPort }}
{{ if .AdvertiseHost }}
client_advertise: "{{ hostport .AdvertiseHost .ClientPort }}"
{{ end }}
{{ if .LogFile }}
log_file: {{ .LogFile }}
{{ end }}

{{ if .JetStream }}
jetstream {
	enabled: true
	store_dir: {{ .StoreDir }}
	{{ if .Snippets.jetstream }}include "{{ .Snippets.jetstream }}"{{ end }}
}
{{ end }}

{{ if and .ClusterName .Routes}}
cluster {
	name: {{ .ClusterName }}
	port: {{ .ClusterPort }}
	routes: [
{{ range .Routes }}
nats://{{ . }}
{{ end }}
	]
}
{{ end }}

{{ if and .Gateways .GatewayPort .ClusterName }}
gateway {
	name: {{ .ClusterName }}
	port: {{ .GatewayPort }}
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

// serverPlan bundles the per-server inputs each create handler computes
// (allocated ports, server directory, optional cluster/gateway wiring) so the
// three handlers can hand off to populateTemplateData uniformly.
type serverPlan struct {
	name          string
	serverDir     string
	serverIndex   int
	clusterIndex  int
	clientPort    int
	advertiseHost string
	jetStream     bool

	clusterName string
	clusterPort int
	routes      []string
	clusterSize int

	clusters    []string
	gatewayPort int
	gateways    map[string][]string

	listenerPorts map[string]int

	tlsInclude string
	tlsFiles   *tlsInstanceFiles
}

// populateTemplateData builds the template env for one server. Cluster and
// gateway fields stay at their zero values for non-cluster / non-super-cluster
// instances.
func populateTemplateData(inst *instance, plan serverPlan) *templateData {
	td := defaultTemplateData()

	td.ServerName = plan.name
	td.ShortID = shortID(inst.ID)
	td.InstanceID = inst.ID
	td.ServerIndex = plan.serverIndex
	td.ServerDir = plan.serverDir
	td.StoreDir = plan.serverDir
	td.LogFile = filepath.Join(plan.serverDir, "server.log")
	td.Host = "localhost"
	td.AdvertiseHost = plan.advertiseHost
	td.ClientPort = plan.clientPort
	td.JetStream = plan.jetStream

	td.Description = inst.Description
	td.Kind = inst.Kind

	if plan.clusterName != "" {
		td.ClusterName = plan.clusterName
		td.ClusterIndex = plan.clusterIndex
		td.ClusterPort = plan.clusterPort
		td.Routes = plan.routes
		td.ClusterSize = plan.clusterSize
	}

	if plan.gatewayPort != 0 {
		td.GatewayPort = plan.gatewayPort
		td.Gateways = plan.gateways
		td.Clusters = plan.clusters
	}

	maps.Copy(td.Ports, plan.listenerPorts)

	td.TLSInclude = plan.tlsInclude
	if plan.tlsFiles != nil {
		td.TLS = &templateTLS{
			CAFile:   plan.tlsFiles.caPath,
			CertFile: plan.tlsFiles.serverCert,
			KeyFile:  plan.tlsFiles.serverKey,
		}
	}

	return td
}

// reserveListenerPorts reserves one TCP port per caller-declared listener
// name. The returned listeners stay open; the caller must add them to its
// heldListeners slice so the close-on-handover race fix still applies.
func (s *Service) reserveListenerPorts(names []string) (map[string]int, []*net.TCPListener, error) {
	if len(names) == 0 {
		return nil, nil, nil
	}
	ports := map[string]int{}
	held := make([]*net.TCPListener, 0, len(names))
	for _, name := range names {
		if name == "" {
			closeListeners(held)
			return nil, nil, fmt.Errorf("listener name must not be empty")
		}
		if _, dup := ports[name]; dup {
			closeListeners(held)
			return nil, nil, fmt.Errorf("duplicate listener name %q", name)
		}
		p, ln, err := s.reservePort()
		if err != nil {
			closeListeners(held)
			return nil, nil, fmt.Errorf("listener %q: %w", name, err)
		}
		ports[name] = p
		held = append(held, ln)
	}
	return ports, held, nil
}

// renderAndWriteSnippets renders each user-supplied snippet body through
// text/template against td, writes the result to <snippetsDir>/<name>.conf
// (mode 0600), and sets td.Snippets[name] to the path used inside the main
// template's include directive — relative to the rendered config file's
// directory, since the NATS conf parser always joins includes onto the
// config-file dir (filepath.Join in conf/parse.go strips the leading slash
// from absolute paths). The config file lands in <serverDir> and snippets
// live at <serverDir>/snippets/<name>.conf, so the include path is
// snippets/<name>.conf.
func renderAndWriteSnippets(td *templateData, snippets map[string]string, snippetsDir string) error {
	snippetsBase := filepath.Base(snippetsDir)
	for name, body := range snippets {
		out := bytes.NewBuffer(nil)
		t, err := template.New("snippet-" + name).Parse(body)
		if err != nil {
			return fmt.Errorf("snippet %q parse: %w", name, err)
		}
		if err := t.Execute(out, td); err != nil {
			return fmt.Errorf("snippet %q render: %w", name, err)
		}
		path := filepath.Join(snippetsDir, name+".conf")
		if err := os.WriteFile(path, out.Bytes(), 0600); err != nil {
			return fmt.Errorf("snippet %q write: %w", name, err)
		}
		td.Snippets[name] = filepath.Join(snippetsBase, name+".conf")
	}
	return nil
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

// setupInstanceTLS generates cert material, writes the shared CA + server
// cert + server key under <inst.RootDir>/tls/, and returns the on-disk paths
// (used by per-server managed snippets) plus the api.TLSMaterial that goes
// into the create response. advertiseHost is the host the servers advertise to
// clients (the resolved --advertise value); it is added to the server cert SANs
// so cross-server TLS discovery to the advertised address verifies. Because it
// derives from the service-wide --advertise flag, every instance minted while
// that flag is set carries the host in its SANs.
func (s *Service) setupInstanceTLS(inst *instance, opts *api.TLSOptions, advertiseHost string) (*tlsInstanceFiles, *api.TLSMaterial, error) {
	mutual := opts.Mode != api.TLSModeServer

	sans := effectiveSANs(opts.SANs, advertiseHost)
	s.log.Info("Issuing TLS server cert", "instance", inst.ID, "sans", sans)

	mat, err := generateTLSMaterial(sans, mutual)
	if err != nil {
		return nil, nil, fmt.Errorf("tls material: %w", err)
	}

	tlsDir := filepath.Join(inst.RootDir, "tls")
	if err := os.MkdirAll(tlsDir, 0700); err != nil {
		return nil, nil, fmt.Errorf("tls dir: %w", err)
	}

	files := &tlsInstanceFiles{
		caPath:         filepath.Join(tlsDir, "ca.pem"),
		serverCert:     filepath.Join(tlsDir, "server.crt"),
		serverKey:      filepath.Join(tlsDir, "server.key"),
		mutual:         mutual,
		handshakeFirst: opts.HandshakeFirst,
		timeoutSeconds: opts.Timeout,
	}

	if err := os.WriteFile(files.caPath, mat.CAPEM, 0644); err != nil {
		return nil, nil, fmt.Errorf("write ca: %w", err)
	}
	if err := os.WriteFile(files.serverCert, mat.ServerCertPEM, 0644); err != nil {
		return nil, nil, fmt.Errorf("write server cert: %w", err)
	}
	if err := os.WriteFile(files.serverKey, mat.ServerKeyPEM, 0600); err != nil {
		return nil, nil, fmt.Errorf("write server key: %w", err)
	}

	apiMat := &api.TLSMaterial{CAPEM: string(mat.CAPEM)}
	if mutual {
		apiMat.ClientCertPEM = string(mat.ClientCertPEM)
		apiMat.ClientKeyPEM = string(mat.ClientKeyPEM)
	}

	return files, apiMat, nil
}

// writeServerTLSSnippet renders the per-server managed TLS snippet — a tls{}
// block with absolute cert paths — into the server's snippets dir. Returns the
// path relative to <serverDir>, matching the convention used by
// renderAndWriteSnippets for the user-supplied slots. Absolute cert paths
// sidestep any ambiguity about how the NATS conf parser resolves relative paths
// inside an included file. client_advertise is owned by the main template
// (driven by --advertise), so the managed snippet does not emit it.
func writeServerTLSSnippet(snippetsDir string, files *tlsInstanceFiles) (string, error) {
	verify := "false"
	if files.mutual {
		verify = "true"
	}
	handshakeFirst := ""
	if files.handshakeFirst {
		handshakeFirst = "\n    handshake_first: true"
	}
	// timeoutSeconds <= 0 means "unset": fall back to the managed default of 2
	// seconds (also nats-server's own default). NaN/Inf can't reach here — the
	// JSON decoder rejects them before the request is handled.
	secs := files.timeoutSeconds
	if secs <= 0 {
		secs = 2
	}
	timeout := strconv.FormatFloat(secs, 'f', -1, 64)
	body := fmt.Sprintf(`tls {
    cert_file: "%s"
    key_file:  "%s"
    ca_file:   "%s"
    verify:    %s
    timeout:   %s%s
}
`, files.serverCert, files.serverKey, files.caPath, verify, timeout, handshakeFirst)

	path := filepath.Join(snippetsDir, "_tls_managed.conf")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		return "", err
	}
	return filepath.Join(filepath.Base(snippetsDir), "_tls_managed.conf"), nil
}

func (s *Service) createServer(req micro.Request) {
	s.log.Info("Handling create server request")

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

	// The capture proxy is a plaintext forwarder, so it cannot front a TLS client port.
	if creq.Trace && creq.TLS != nil {
		req.Error("013", "trace capture does not support TLS instances", nil)
		return
	}

	if creq.Trace && s.capturer == nil {
		req.Error("014", "trace capture is not configured on this service", nil)
		return
	}

	inst := s.newInstance("server", creq.Description)
	short := shortID(inst.ID)

	// Listeners reserved for the lifetime of this create call. Closed right
	// before server.Start() so that concurrent create calls cannot race onto
	// the same ports.
	var heldListeners []*net.TCPListener
	rollback := func() {
		closeListeners(heldListeners)
		s.dropInstance(inst.ID)
	}

	if err := os.MkdirAll(inst.RootDir, 0700); err != nil {
		rollback()
		req.Error("002", fmt.Sprintf("Failed to create instance dir: %v", err), nil)
		return
	}

	advertiseHost := effectiveAdvertiseHost(s.advertiseHost, creq.TLS)

	var tlsFiles *tlsInstanceFiles
	var tlsResp *api.TLSMaterial
	if creq.TLS != nil {
		var err error
		tlsFiles, tlsResp, err = s.setupInstanceTLS(inst, creq.TLS, advertiseHost)
		if err != nil {
			rollback()
			req.Error("008", fmt.Sprintf("TLS setup failed: %v", err), nil)
			return
		}
	}

	name := short + "-n1"
	sd := filepath.Join(inst.RootDir, "n1")
	snippetsDir := filepath.Join(sd, "snippets")
	if err := os.MkdirAll(snippetsDir, 0700); err != nil {
		rollback()
		req.Error("002", "Failed to create server directory", nil)
		return
	}

	clientPort, clientLn, err := s.reservePort()
	if err != nil {
		rollback()
		req.Error("006", fmt.Sprintf("could not get free port: %v", err), nil)
		return
	}
	heldListeners = append(heldListeners, clientLn)

	listenerPorts, listenerLns, err := s.reserveListenerPorts(listenersForSnippets(creq.Snippets))
	if err != nil {
		rollback()
		req.Error("006", err.Error(), nil)
		return
	}
	heldListeners = append(heldListeners, listenerLns...)

	var tlsInclude string
	if tlsFiles != nil {
		tlsInclude, err = writeServerTLSSnippet(snippetsDir, tlsFiles)
		if err != nil {
			rollback()
			req.Error("008", fmt.Sprintf("TLS snippet failed: %v", err), nil)
			return
		}
	}

	td := populateTemplateData(inst, serverPlan{
		name:          name,
		serverDir:     sd,
		serverIndex:   1,
		clientPort:    clientPort,
		advertiseHost: advertiseHost,
		jetStream:     creq.JetStream,
		listenerPorts: listenerPorts,
		tlsInclude:    tlsInclude,
		tlsFiles:      tlsFiles,
	})

	if err := renderAndWriteSnippets(td, creq.Snippets, snippetsDir); err != nil {
		rollback()
		req.Error("002", fmt.Sprintf("Snippet failure: %v", err), nil)
		return
	}

	serverConfig, err := renderConfig(td, mainTemplate)
	if err != nil {
		rollback()
		req.Error("002", fmt.Sprintf("Template parse failure: %v", err), nil)
		return
	}

	srv, cfgPath, err := runServerWithConfig(s.log, serverConfig, sd, heldListeners)
	heldListeners = nil // listeners are closed inside runServerWithConfig
	if err != nil {
		rollback()
		req.Error("003", fmt.Sprintf("Server creation failed: %v", err), nil)
		return
	}

	port, err := clientPortOf(srv)
	if err != nil {
		s.log.Warn("Could not parse client port", "server", name, "err", err)
	}

	ms := &managedServer{srv: srv, rootDir: sd, configPath: cfgPath, ports: listenerPorts, clientPort: port, advertiseHost: advertiseHost, td: td, tlsFiles: tlsFiles}

	// Front the client port with a capture proxy when requested, before the server is
	// published so its trace port and proxy are set atomically.
	if creq.Trace {
		proxy, tracePort, terr := s.startTraceProxy(inst, name, port, advertiseHost)
		if terr != nil {
			// srv is not in inst.Servers yet, so rollback's teardown would not stop it;
			// shut it down here to avoid leaking the started server on trace failure.
			srv.Shutdown()
			srv.WaitForShutdown()
			rollback()
			req.Error("012", fmt.Sprintf("trace setup failed: %v", terr), nil)
			return
		}
		ms.traceProxy = proxy
		if listenerPorts == nil {
			listenerPorts = map[string]int{}
		}
		listenerPorts["trace"] = tracePort
		ms.ports = listenerPorts
	}

	s.mu.Lock()
	inst.Servers = append(inst.Servers, ms)
	s.mu.Unlock()

	req.RespondJSON(api.CreateResponse{
		ID:          inst.ID,
		Description: inst.Description,
		Kind:        inst.Kind,
		Servers: []*api.ManagedServer{
			{Name: name, Port: port, Ports: listenerPorts, Advertise: advertiseHost, Running: srv.Running()},
		},
		TLS: tlsResp,
	})
}

// traceSetupTimeout limits a Capturer's setup for one server, which may include
// first-use work such as creating the store captures are written to.
const traceSetupTimeout = 30 * time.Second

// startTraceProxy asks the configured Capturer to front a managed server's client
// port with a capture proxy, and returns the proxy and the port clients reach it on.
//
// advertiseHost becomes the proxy's listen host, so the advertised trace URL matches
// the node's client_advertise. It must name a local interface or the capturer fails
// to listen. Empty binds and advertises 0.0.0.0 (the no-advertise default). The
// caller must shut down the backend server on failure: it is not yet tracked by the
// instance.
func (s *Service) startTraceProxy(inst *instance, serverName string, clientPort int, advertiseHost string) (CaptureProxy, int, error) {
	if s.capturer == nil {
		return nil, 0, fmt.Errorf("trace capture is not configured on this service")
	}

	ctx, cancel := context.WithTimeout(context.Background(), traceSetupTimeout)
	defer cancel()

	tmpDir := filepath.Join(inst.RootDir, "traces")
	if err := os.MkdirAll(tmpDir, 0700); err != nil {
		return nil, 0, err
	}

	proxy, err := s.capturer.Capture(ctx, CaptureRequest{
		InstanceID: inst.ID,
		ServerName: serverName,
		Backend:    fmt.Sprintf("127.0.0.1:%d", clientPort),
		ListenHost: advertiseHost,
		TmpDir:     tmpDir,
	})
	if err != nil {
		return nil, 0, err
	}

	// Empty advertiseHost binds 0.0.0.0 inside the proxy; mirror that here so the log
	// reports the host actually bound.
	host := advertiseHost
	if host == "" {
		host = "0.0.0.0"
	}
	s.log.Info("Trace proxy listening", "server", serverName, "address", net.JoinHostPort(host, strconv.Itoa(proxy.Port())))
	return proxy, proxy.Port(), nil
}

func (s *Service) createCluster(req micro.Request) {
	s.log.Info("Handling create cluster request")
	start := time.Now()
	defer func() { s.log.Info("Handled create cluster request", "duration", time.Since(start)) }()

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

	if creq.Trace && creq.TLS != nil {
		req.Error("013", "trace capture does not support TLS instances", nil)
		return
	}

	if creq.Trace && s.capturer == nil {
		req.Error("014", "trace capture is not configured on this service", nil)
		return
	}

	inst := s.newInstance("cluster", creq.Description)
	short := shortID(inst.ID)
	clusterName := "C_" + short
	inst.Cluster = clusterName

	// One route listener per node, reserved up front because every node's config
	// lists the full set of route URLs. Each entry stays held until ITS node
	// starts: it is moved into that node's hand-off set (and cleared here) right
	// before that node's runServerWithConfig closes it. Closing them all on the
	// first node's start — the previous behavior — freed the later nodes' route
	// ports into the hand-over gap, where another spawn could grab the port and
	// crash the binding node.
	routeLns := make([]*net.TCPListener, creq.Servers)
	rollback := func() {
		closeListeners(routeLns) // route listeners for nodes that have not started
		s.dropInstance(inst.ID)
	}

	if err := os.MkdirAll(inst.RootDir, 0700); err != nil {
		rollback()
		req.Error("002", fmt.Sprintf("Failed to create instance dir: %v", err), nil)
		return
	}

	advertiseHost := effectiveAdvertiseHost(s.advertiseHost, creq.TLS)

	var tlsFiles *tlsInstanceFiles
	var tlsResp *api.TLSMaterial
	if creq.TLS != nil {
		var err error
		tlsFiles, tlsResp, err = s.setupInstanceTLS(inst, creq.TLS, advertiseHost)
		if err != nil {
			rollback()
			req.Error("008", fmt.Sprintf("TLS setup failed: %v", err), nil)
			return
		}
	}

	clusterPorts := make([]int, creq.Servers)
	clusterUrls := make([]string, creq.Servers)
	for i := 0; i < creq.Servers; i++ {
		p, ln, err := s.reservePort()
		if err != nil {
			rollback()
			req.Error("006", fmt.Sprintf("could not get free port: %v", err), nil)
			return
		}
		clusterPorts[i] = p
		clusterUrls[i] = fmt.Sprintf("localhost:%d", p)
		routeLns[i] = ln
	}

	resp := api.CreateResponse{
		ID:          inst.ID,
		Description: inst.Description,
		Kind:        inst.Kind,
		Servers:     []*api.ManagedServer{},
		TLS:         tlsResp,
	}

	for i := 1; i <= creq.Servers; i++ {
		// Hand-off set for this node: its own route listener plus the client
		// (and snippet) listeners reserved below. runServerWithConfig closes
		// exactly these right before Start(); the other nodes' route listeners
		// stay held until their own iteration.
		nodeLns := []*net.TCPListener{routeLns[i-1]}
		routeLns[i-1] = nil // ownership moved into nodeLns

		name := fmt.Sprintf("%s-n%d", short, i)
		sd := filepath.Join(inst.RootDir, fmt.Sprintf("n%d", i))
		snippetsDir := filepath.Join(sd, "snippets")
		if err := os.MkdirAll(snippetsDir, 0700); err != nil {
			closeListeners(nodeLns)
			rollback()
			req.Error("002", "Failed to create server directory", nil)
			return
		}

		clientPort, clientLn, err := s.reservePort()
		if err != nil {
			closeListeners(nodeLns)
			rollback()
			req.Error("006", fmt.Sprintf("could not get free port: %v", err), nil)
			return
		}
		nodeLns = append(nodeLns, clientLn)

		listenerPorts, listenerLns, err := s.reserveListenerPorts(listenersForSnippets(creq.Snippets))
		if err != nil {
			closeListeners(nodeLns)
			rollback()
			req.Error("006", err.Error(), nil)
			return
		}
		nodeLns = append(nodeLns, listenerLns...)

		var tlsInclude string
		if tlsFiles != nil {
			tlsInclude, err = writeServerTLSSnippet(snippetsDir, tlsFiles)
			if err != nil {
				rollback()
				req.Error("008", fmt.Sprintf("TLS snippet failed: %v", err), nil)
				return
			}
		}

		td := populateTemplateData(inst, serverPlan{
			name:          name,
			serverDir:     sd,
			serverIndex:   i,
			clientPort:    clientPort,
			advertiseHost: advertiseHost,
			jetStream:     creq.JetStream,
			clusterName:   clusterName,
			clusterPort:   clusterPorts[i-1],
			routes:        clusterUrls,
			clusterSize:   creq.Servers,
			listenerPorts: listenerPorts,
			tlsInclude:    tlsInclude,
			tlsFiles:      tlsFiles,
		})

		if err := renderAndWriteSnippets(td, creq.Snippets, snippetsDir); err != nil {
			closeListeners(nodeLns)
			rollback()
			req.Error("002", fmt.Sprintf("Snippet failure: %v", err), nil)
			return
		}

		serverConfig, err := renderConfig(td, mainTemplate)
		if err != nil {
			closeListeners(nodeLns)
			rollback()
			req.Error("002", fmt.Sprintf("Template parse failure: %v", err), nil)
			return
		}

		// nodeLns (incl. this node's route listener) are closed inside
		// runServerWithConfig on every path; routeLns then holds only the route
		// listeners of nodes that have not started yet.
		srv, cfgPath, err := runServerWithConfig(s.log, serverConfig, sd, nodeLns)
		if err != nil {
			rollback()
			req.Error("003", fmt.Sprintf("Server creation failed: %v", err), nil)
			return
		}

		port, err := clientPortOf(srv)
		if err != nil {
			s.log.Warn("Could not parse client port", "server", name, "err", err)
		}

		ms := &managedServer{srv: srv, rootDir: sd, configPath: cfgPath, ports: listenerPorts, clientPort: port, advertiseHost: advertiseHost, td: td, tlsFiles: tlsFiles}

		// A single capture proxy fronts the first node of the cluster.
		if creq.Trace && i == 1 {
			proxy, tracePort, terr := s.startTraceProxy(inst, name, port, advertiseHost)
			if terr != nil {
				// srv is not in inst.Servers yet, so rollback's teardown would not stop it;
				// shut it down here to avoid leaking the started server on trace failure.
				srv.Shutdown()
				srv.WaitForShutdown()
				rollback()
				req.Error("012", fmt.Sprintf("trace setup failed: %v", terr), nil)
				return
			}
			ms.traceProxy = proxy
			if listenerPorts == nil {
				listenerPorts = map[string]int{}
			}
			listenerPorts["trace"] = tracePort
			ms.ports = listenerPorts
		}

		s.mu.Lock()
		inst.Servers = append(inst.Servers, ms)
		s.mu.Unlock()

		resp.Servers = append(resp.Servers, &api.ManagedServer{
			Name:      name,
			Port:      port,
			Ports:     listenerPorts,
			Cluster:   clusterName,
			Advertise: advertiseHost,
			Running:   srv.Running(),
		})
	}

	req.RespondJSON(resp)
}

func (s *Service) createSuperCluster(req micro.Request) {
	s.log.Info("Handling create super cluster request")

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

	if creq.Clusters < 2 {
		req.Error("004", "Invalid request: clusters should be at least 2", nil)
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

	if creq.Trace && creq.TLS != nil {
		req.Error("013", "trace capture does not support TLS instances", nil)
		return
	}

	if creq.Trace && s.capturer == nil {
		req.Error("014", "trace capture is not configured on this service", nil)
		return
	}

	inst := s.newInstance("super-cluster", creq.Description)
	short := shortID(inst.ID)

	// One gateway listener per node in every cluster, and one route listener per
	// node within each cluster. Both are reserved before the nodes that bind them
	// exist, because every node's config lists the full set of route and gateway
	// URLs. Each entry stays held until ITS node starts: the node moves it into
	// its own hand-off set (clearing the slot here) right before that node's
	// runServerWithConfig closes it. Closing them all on the first node's start
	// would free every later node's route and gateway port into the hand-over
	// gap, where another spawn could grab the port and crash the binding node.
	gatewayLns := make([][]*net.TCPListener, creq.Clusters)
	routeLns := make([][]*net.TCPListener, creq.Clusters)
	rollback := func() {
		for _, lns := range gatewayLns {
			closeListeners(lns) // gateway listeners for nodes that have not started
		}
		for _, lns := range routeLns {
			closeListeners(lns) // route listeners for nodes that have not started
		}
		s.dropInstance(inst.ID)
	}

	if err := os.MkdirAll(inst.RootDir, 0700); err != nil {
		rollback()
		req.Error("002", fmt.Sprintf("Failed to create instance dir: %v", err), nil)
		return
	}

	advertiseHost := effectiveAdvertiseHost(s.advertiseHost, creq.TLS)

	var tlsFiles *tlsInstanceFiles
	var tlsResp *api.TLSMaterial
	if creq.TLS != nil {
		var err error
		tlsFiles, tlsResp, err = s.setupInstanceTLS(inst, creq.TLS, advertiseHost)
		if err != nil {
			rollback()
			req.Error("008", fmt.Sprintf("TLS setup failed: %v", err), nil)
			return
		}
	}

	resp := api.CreateResponse{
		ID:          inst.ID,
		Description: inst.Description,
		Kind:        inst.Kind,
		Servers:     []*api.ManagedServer{},
		TLS:         tlsResp,
	}

	clusterNames := make([]string, creq.Clusters)
	for c := 1; c <= creq.Clusters; c++ {
		clusterNames[c-1] = fmt.Sprintf("SC_%s_%d", short, c)
	}

	// Allocate gateway ports for every cluster up front so each cluster's
	// configuration can reference all the others' gateway URLs.
	superClusterPorts := map[string][]int{}
	superClusterUrls := map[string][]string{}
	for ci, name := range clusterNames {
		ports := make([]int, creq.Servers)
		urls := make([]string, creq.Servers)
		gatewayLns[ci] = make([]*net.TCPListener, creq.Servers)

		for i := 0; i < creq.Servers; i++ {
			p, ln, err := s.reservePort()
			if err != nil {
				rollback()
				req.Error("006", fmt.Sprintf("could not get free port: %v", err), nil)
				return
			}
			ports[i] = p
			urls[i] = fmt.Sprintf("localhost:%d", p)
			gatewayLns[ci][i] = ln
		}

		superClusterPorts[name] = ports
		superClusterUrls[name] = urls
	}

	for c := 1; c <= creq.Clusters; c++ {
		clusterName := clusterNames[c-1]

		clusterPorts := make([]int, creq.Servers)
		clusterUrls := make([]string, creq.Servers)
		routeLns[c-1] = make([]*net.TCPListener, creq.Servers)
		for i := 0; i < creq.Servers; i++ {
			p, ln, err := s.reservePort()
			if err != nil {
				rollback()
				req.Error("006", fmt.Sprintf("could not get free port: %v", err), nil)
				return
			}
			clusterPorts[i] = p
			clusterUrls[i] = fmt.Sprintf("localhost:%d", p)
			routeLns[c-1][i] = ln
		}

		for i := 1; i <= creq.Servers; i++ {
			// Hand-off set for this node: its own route and gateway listeners plus
			// the client (and snippet) listeners reserved below. runServerWithConfig
			// closes exactly these right before Start(); the other nodes' listeners
			// stay held until their own iteration.
			nodeLns := []*net.TCPListener{routeLns[c-1][i-1], gatewayLns[c-1][i-1]}
			routeLns[c-1][i-1] = nil   // ownership moved into nodeLns
			gatewayLns[c-1][i-1] = nil // ownership moved into nodeLns

			name := fmt.Sprintf("%s-c%d_s%d", short, c, i)
			sd := filepath.Join(inst.RootDir, fmt.Sprintf("c%d_s%d", c, i))
			snippetsDir := filepath.Join(sd, "snippets")
			if err := os.MkdirAll(snippetsDir, 0700); err != nil {
				closeListeners(nodeLns)
				rollback()
				req.Error("002", "Failed to create server directory", nil)
				return
			}

			clientPort, clientLn, err := s.reservePort()
			if err != nil {
				closeListeners(nodeLns)
				rollback()
				req.Error("006", fmt.Sprintf("could not get free port: %v", err), nil)
				return
			}
			nodeLns = append(nodeLns, clientLn)

			listenerPorts, listenerLns, err := s.reserveListenerPorts(listenersForSnippets(creq.Snippets))
			if err != nil {
				closeListeners(nodeLns)
				rollback()
				req.Error("006", err.Error(), nil)
				return
			}
			nodeLns = append(nodeLns, listenerLns...)

			var tlsInclude string
			if tlsFiles != nil {
				tlsInclude, err = writeServerTLSSnippet(snippetsDir, tlsFiles)
				if err != nil {
					closeListeners(nodeLns)
					rollback()
					req.Error("008", fmt.Sprintf("TLS snippet failed: %v", err), nil)
					return
				}
			}

			td := populateTemplateData(inst, serverPlan{
				name:          name,
				serverDir:     sd,
				serverIndex:   i,
				clusterIndex:  c,
				clientPort:    clientPort,
				advertiseHost: advertiseHost,
				jetStream:     creq.JetStream,
				clusterName:   clusterName,
				clusterPort:   clusterPorts[i-1],
				routes:        clusterUrls,
				clusterSize:   creq.Servers,
				clusters:      clusterNames,
				gatewayPort:   superClusterPorts[clusterName][i-1],
				gateways:      superClusterUrls,
				listenerPorts: listenerPorts,
				tlsInclude:    tlsInclude,
				tlsFiles:      tlsFiles,
			})

			if err := renderAndWriteSnippets(td, creq.Snippets, snippetsDir); err != nil {
				closeListeners(nodeLns)
				rollback()
				req.Error("002", fmt.Sprintf("Snippet failure: %v", err), nil)
				return
			}

			serverConfig, err := renderConfig(td, mainTemplate)
			if err != nil {
				closeListeners(nodeLns)
				rollback()
				req.Error("002", fmt.Sprintf("Template parse failure: %v", err), nil)
				return
			}

			// nodeLns (incl. this node's route and gateway listeners) are closed
			// inside runServerWithConfig on every path; routeLns and gatewayLns then
			// hold only the listeners of nodes that have not started yet.
			srv, cfgPath, err := runServerWithConfig(s.log, serverConfig, sd, nodeLns)
			if err != nil {
				rollback()
				req.Error("003", fmt.Sprintf("Server creation failed: %v", err), nil)
				return
			}

			port, err := clientPortOf(srv)
			if err != nil {
				s.log.Warn("Could not parse client port", "server", name, "err", err)
			}

			ms := &managedServer{srv: srv, rootDir: sd, configPath: cfgPath, ports: listenerPorts, clientPort: port, advertiseHost: advertiseHost, td: td, tlsFiles: tlsFiles}

			// A single capture proxy fronts the first node of the first cluster.
			if creq.Trace && c == 1 && i == 1 {
				proxy, tracePort, terr := s.startTraceProxy(inst, name, port, advertiseHost)
				if terr != nil {
					// srv is not in inst.Servers yet, so rollback's teardown would not stop it;
					// shut it down here to avoid leaking the started server on trace failure.
					srv.Shutdown()
					srv.WaitForShutdown()
					rollback()
					req.Error("012", fmt.Sprintf("trace setup failed: %v", terr), nil)
					return
				}
				ms.traceProxy = proxy
				if listenerPorts == nil {
					listenerPorts = map[string]int{}
				}
				listenerPorts["trace"] = tracePort
				ms.ports = listenerPorts
			}

			s.mu.Lock()
			inst.Servers = append(inst.Servers, ms)
			s.mu.Unlock()

			resp.Servers = append(resp.Servers, &api.ManagedServer{
				Name:      name,
				Port:      port,
				Ports:     listenerPorts,
				Cluster:   clusterName,
				Advertise: advertiseHost,
				Running:   srv.Running(),
			})
		}
	}

	req.RespondJSON(resp)
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

	// Hold cfgMu so a concurrent start cannot replace ms.srv while it is read and
	// shut down here: startServer holds cfgMu across its ms.srv store.
	ms.cfgMu.Lock()
	defer ms.cfgMu.Unlock()

	if !ms.srv.Running() {
		req.Error("003", "Server not running", nil)
		return
	}

	ms.srv.Shutdown()
	ms.srv.WaitForShutdown()
	if ms.srv.Running() {
		req.Error("004", "Server did not shut down", nil)
		return
	}

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

	if ms.srv.Running() {
		req.Error("003", "Server already running", nil)
		return
	}

	if ms.configPath == "" {
		req.Error("005", "Server cannot be restarted: no persisted config", nil)
		return
	}

	// Hold cfgMu while reading configPath so a concurrent update can't be
	// mid-write when ProcessConfigFile parses it.
	ms.cfgMu.Lock()
	srv, err := startFromConfig(s.log, ms.configPath)
	if err != nil {
		ms.cfgMu.Unlock()
		req.Error("005", fmt.Sprintf("Server restart failed: %v", err), nil)
		return
	}

	s.mu.Lock()
	ms.srv = srv
	s.mu.Unlock()
	ms.cfgMu.Unlock()

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

	results := make([]api.ServerStateResult, 0, len(servers))
	for _, ms := range servers {
		ms.cfgMu.Lock()
		srv := ms.srv
		if srv != nil && srv.Running() {
			srv.Shutdown()
			srv.WaitForShutdown()
		}
		res := api.ServerStateResult{}
		if srv != nil {
			res.Name = srv.Name()
			res.Running = srv.Running()
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

	results := make([]api.ServerStateResult, 0, len(servers))
	for _, ms := range servers {
		res := api.ServerStateResult{}

		ms.cfgMu.Lock()
		if ms.srv != nil {
			res.Name = ms.srv.Name()
		}

		if ms.srv != nil && ms.srv.Running() {
			res.Running = true
			ms.cfgMu.Unlock()
			results = append(results, res)
			continue
		}

		if ms.configPath == "" {
			ms.cfgMu.Unlock()
			res.Error = "no persisted config"
			results = append(results, res)
			continue
		}

		srv, err := startFromConfig(s.log, ms.configPath)
		if err != nil {
			ms.cfgMu.Unlock()
			res.Error = err.Error()
			results = append(results, res)
			continue
		}

		// A concurrent destroy may have removed the instance while this node was
		// starting. If so, shut the freshly-started server down rather than leak
		// it, and stop.
		s.mu.Lock()
		_, alive := s.instances[creq.InstanceID]
		if alive {
			ms.srv = srv
		}
		s.mu.Unlock()
		ms.cfgMu.Unlock()

		if !alive {
			srv.Shutdown()
			srv.WaitForShutdown()
			req.Error("404", "Instance destroyed during start", nil)
			return
		}

		res.Name = srv.Name()
		res.Running = srv.Running()
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

	if err := validateTLSSnippetRefs(creq.Snippets, ms.td.TLS != nil); err != nil {
		req.Error("007", err.Error(), nil)
		return
	}

	if err := validateJetStreamSnippet(creq.Snippets, ms.td.JetStream); err != nil {
		req.Error("011", err.Error(), nil)
		return
	}

	if err := validateListenerPortSet(creq.Snippets, ms.ports); err != nil {
		req.Error("008", err.Error(), nil)
		return
	}

	if creq.TLSTimeout != nil && ms.tlsFiles == nil {
		req.Error("007", "server was not created with WithGeneratedTLS; cannot set tls_timeout", nil)
		return
	}

	tdNew := cloneTemplateData(ms.td)
	snippetsDir := filepath.Join(ms.rootDir, "snippets")

	if err := renderAndWriteSnippets(tdNew, creq.Snippets, snippetsDir); err != nil {
		req.Error("009", fmt.Sprintf("snippet render failed: %v", err), nil)
		return
	}

	var newTLS *tlsInstanceFiles
	if creq.TLSTimeout != nil {
		cow := *ms.tlsFiles
		cow.timeoutSeconds = *creq.TLSTimeout
		if _, err := writeServerTLSSnippet(snippetsDir, &cow); err != nil {
			req.Error("009", fmt.Sprintf("TLS snippet rewrite failed: %v", err), nil)
			return
		}
		newTLS = &cow
	}

	mainTmpl := serverConfigTemplate
	if creq.Template != "" {
		mainTmpl = creq.Template
	}
	rendered, err := renderConfig(tdNew, mainTmpl)
	if err != nil {
		req.Error("009", fmt.Sprintf("config render failed: %v", err), nil)
		return
	}

	if err := os.WriteFile(ms.configPath, rendered, 0600); err != nil {
		req.Error("009", fmt.Sprintf("config write failed: %v", err), nil)
		return
	}

	// Writing new config was successful, update the managedServer's templateData.
	ms.td = tdNew
	if newTLS != nil {
		ms.tlsFiles = newTLS
	}

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
	// concurrent start from replacing ms.srv underneath them, keeps the server
	// from stopping between the two, and stops Reload() (which re-reads
	// configPath) observing a torn file mid-write from a concurrent update.
	ms.cfgMu.Lock()
	defer ms.cfgMu.Unlock()

	if !ms.srv.Running() {
		req.Error("002", "Server not running", nil)
		return
	}

	if err := ms.srv.Reload(); err != nil {
		req.Error("010", fmt.Sprintf("reload failed: %v", err), nil)
		return
	}

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

	if creq.InstanceID != "" && len(snapshots) == 0 {
		req.Error("404", "Instance not found", nil)
		return
	}

	resp := api.StatusResponse{Instances: make([]api.InstanceStatus, 0, len(snapshots))}
	for _, snap := range snapshots {
		ist := api.InstanceStatus{
			ID:          snap.id,
			Description: snap.description,
			Kind:        snap.kind,
			Servers:     make([]api.ManagedServer, 0, len(snap.servers)),
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
			})
		}
		resp.Instances = append(resp.Instances, ist)
	}

	req.RespondJSON(resp)
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

	s.tearDownInstance(inst)
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

// instanceSnapshot is a lock-released view of an instance's metadata and the
// reportable state of its servers, taken so a status response can be built
// without holding s.mu.
type instanceSnapshot struct {
	id          string
	kind        string
	description string
	servers     []serverSnapshot
}

// serverSnapshot is a managed server's reportable state, read out while the
// caller holds s.mu. It holds no *server.Server: startServer and startInstance
// replace that pointer under s.mu, so reading it afterwards would race them.
type serverSnapshot struct {
	name          string
	cluster       string
	clientPort    int
	ports         map[string]int
	advertiseHost string
	running       bool
	clusterPort   int // 0 when the server has no cluster listener
}

// snapshotInstance reads an instance and its servers into plain values. The
// caller must hold s.mu.
func snapshotInstance(inst *instance) instanceSnapshot {
	srvs := make([]serverSnapshot, 0, len(inst.Servers))
	for _, ms := range inst.Servers {
		snap := serverSnapshot{
			// clientPort is captured at create time, so it survives a stop; the
			// route/cluster port is reported separately as clusterPort.
			clientPort:    ms.clientPort,
			ports:         maps.Clone(ms.ports),
			advertiseHost: ms.advertiseHost,
		}
		if ms.srv != nil {
			snap.name = ms.srv.Name()
			snap.cluster = ms.srv.ClusterName()
			snap.running = ms.srv.Running()
			if addr := ms.srv.ClusterAddr(); addr != nil {
				snap.clusterPort = addr.Port
			}
		}
		srvs = append(srvs, snap)
	}

	return instanceSnapshot{
		id:          inst.ID,
		kind:        inst.Kind,
		description: inst.Description,
		servers:     srvs,
	}
}

// findServerByName scans every instance for a server whose runtime name matches.
// Server names are made globally unique by the short-id prefix, so the first
// match is always the right one.
func (s *Service) findServerByName(name string) *managedServer {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, inst := range s.instances {
		for _, ms := range inst.Servers {
			if ms.srv != nil && ms.srv.Name() == name {
				return ms
			}
		}
	}
	return nil
}

// reservePort allocates a free TCP port on the wildcard address (0.0.0.0) and
// returns the port plus the still-open listener holding it. Reserving on the
// same wildcard address the embedded nats-server binds on avoids handing back a
// port that is already live on 0.0.0.0. The caller must close the listener
// immediately before binding the port for real (typically by passing it through
// runServerWithConfig).
func (s *Service) reservePort() (int, *net.TCPListener, error) {
	a, err := net.ResolveTCPAddr("tcp", "0.0.0.0:0")
	if err != nil {
		return 0, nil, err
	}
	l, err := net.ListenTCP("tcp", a)
	if err != nil {
		return 0, nil, err
	}
	return l.Addr().(*net.TCPAddr).Port, l, nil
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

// runServerWithConfig writes the rendered NATS config to a file under rootDir,
// closes the held port-reservation listeners, and starts the server. The
// listeners must remain open through config write and server construction so
// that concurrent create calls cannot grab the same ports.
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

	srv.ConfigureLogger()

	// Hand over the reserved ports to the server: close the listeners
	// immediately before Start() to minimize the window where a concurrent
	// create call could win the same port.
	closeListeners(listeners)

	log.Info("Starting server", "server", srv.Name(), "dir", rootDir)
	srv.Start()

	if !srv.ReadyForConnections(10 * time.Second) {
		return nil, "", errors.New("server failed to start")
	}

	log.Info("Started server", "server", srv.Name(), "url", srv.ClientURL())

	return srv, configPath, nil
}

// startFromConfig rebuilds and starts a server from a previously-written config
// file. Used by tester.start.server to bring a stopped server back online; the
// underlying *server.Server cannot be restarted in place after Shutdown.
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

	srv.ConfigureLogger()

	log.Info("Restarting server", "server", srv.Name())
	srv.Start()

	if !srv.ReadyForConnections(10 * time.Second) {
		return nil, errors.New("server failed to restart")
	}

	log.Info("Restarted server", "server", srv.Name(), "url", srv.ClientURL())

	return srv, nil
}

// cloneTemplateData returns a shallow copy of td with a fresh empty
// Snippets map. Used by updateServer to render against a copy so an
// in-progress update doesn't mutate the live ms.td before the new
// config is written; ms.td is only swapped to the copy on success.
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
}
