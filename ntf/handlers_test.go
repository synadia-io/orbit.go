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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/synadia-io/orbit.go/ntf/api"
)

func TestPopulateTemplateData(t *testing.T) {
	inst := &instance{
		ID:          "abcdef0123456789abcdef0123456789",
		Description: "test",
		Kind:        "server",
	}

	tests := []struct {
		name          string
		advertiseHost string
	}{
		{"copies advertise host", "myhost.example"},
		{"empty advertise host stays empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := serverPlan{
				name:          "test-n1",
				serverDir:     "/tmp/test",
				serverIndex:   1,
				clientPort:    4222,
				advertiseHost: tt.advertiseHost,
			}

			td := populateTemplateData(inst, plan)

			if td.AdvertiseHost != tt.advertiseHost {
				t.Fatalf("AdvertiseHost: got %q, want %q", td.AdvertiseHost, tt.advertiseHost)
			}
		})
	}
}

func TestRenderConfigClientAdvertise(t *testing.T) {
	newTD := func(t *testing.T) *templateData {
		t.Helper()
		td := defaultTemplateData()
		td.ServerName = "test"
		td.ClientPort = 14222
		td.StoreDir = t.TempDir()
		td.LogFile = filepath.Join(td.StoreDir, "server.log")
		return td
	}

	t.Run("emits client_advertise when AdvertiseHost set", func(t *testing.T) {
		td := newTD(t)
		td.AdvertiseHost = "myhost.example"

		out, err := renderConfig(td, serverConfigTemplate)
		if err != nil {
			t.Fatalf("renderConfig: %v", err)
		}

		want := `client_advertise: "myhost.example:14222"`
		if !strings.Contains(string(out), want) {
			t.Fatalf("rendered output missing %q\n---\n%s\n---", want, out)
		}

		cfgPath := filepath.Join(t.TempDir(), "test.cfg")
		if err := os.WriteFile(cfgPath, out, 0600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		opts, err := server.ProcessConfigFile(cfgPath)
		if err != nil {
			t.Fatalf("ProcessConfigFile rejected rendered config: %v", err)
		}
		if opts.ClientAdvertise != "myhost.example:14222" {
			t.Fatalf("opts.ClientAdvertise = %q, want %q", opts.ClientAdvertise, "myhost.example:14222")
		}
	})

	t.Run("omits client_advertise when AdvertiseHost empty", func(t *testing.T) {
		td := newTD(t)

		out, err := renderConfig(td, serverConfigTemplate)
		if err != nil {
			t.Fatalf("renderConfig: %v", err)
		}

		if strings.Contains(string(out), "client_advertise:") {
			t.Fatalf("rendered output unexpectedly contains client_advertise:\n---\n%s\n---", out)
		}
	})
}

func TestEffectiveAdvertiseHost(t *testing.T) {
	tls := &api.TLSOptions{}
	tests := []struct {
		name       string
		configured string
		tls        *api.TLSOptions
		want       string
	}{
		{"configured advertise wins with tls", "myhost", tls, "myhost"},
		{"configured advertise wins without tls", "myhost", nil, "myhost"},
		{"tls without advertise defaults to localhost", "", tls, "localhost"},
		{"no tls and no advertise stays empty", "", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveAdvertiseHost(tt.configured, tt.tls)
			if got != tt.want {
				t.Fatalf("effectiveAdvertiseHost(%q, tls=%v) = %q, want %q", tt.configured, tt.tls != nil, got, tt.want)
			}
		})
	}
}

// TestWriteServerTLSSnippetOmitsClientAdvertise pins that the managed TLS
// snippet no longer emits client_advertise — that directive is owned solely by
// the main template (driven by --advertise), so a snippet copy can't override
// the operator's value.
func TestWriteServerTLSSnippetOmitsClientAdvertise(t *testing.T) {
	dir := t.TempDir()
	files := &tlsInstanceFiles{
		caPath:     "/tls/ca.pem",
		serverCert: "/tls/server.crt",
		serverKey:  "/tls/server.key",
		mutual:     true,
	}

	rel, err := writeServerTLSSnippet(dir, files)
	if err != nil {
		t.Fatalf("writeServerTLSSnippet: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, filepath.Base(rel)))
	if err != nil {
		t.Fatalf("read snippet: %v", err)
	}
	got := string(body)

	if strings.Contains(got, "client_advertise") {
		t.Fatalf("managed TLS snippet must not emit client_advertise:\n%s", got)
	}
	for _, want := range []string{"tls {", `cert_file: "/tls/server.crt"`, "verify:    true"} {
		if !strings.Contains(got, want) {
			t.Fatalf("snippet missing %q:\n%s", want, got)
		}
	}
}

// TestValidateTLSSnippetRefs pins the actionable error when a snippet uses the
// .TLS.* paths without generated TLS, and that it stays quiet otherwise.
func TestValidateTLSSnippetRefs(t *testing.T) {
	wsTLS := map[string]string{"websocket": `tls { cert_file: "{{ .TLS.CertFile }}" }`}

	t.Run("rejects .TLS ref without generated TLS", func(t *testing.T) {
		err := validateTLSSnippetRefs(wsTLS, false)
		if err == nil {
			t.Fatal("expected error referencing .TLS without TLS")
		}
		if !strings.Contains(err.Error(), "WithGeneratedTLS") {
			t.Fatalf("error should name WithGeneratedTLS, got: %v", err)
		}
	})

	t.Run("allows .TLS ref when TLS is active", func(t *testing.T) {
		if err := validateTLSSnippetRefs(wsTLS, true); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("allows snippets that do not reference .TLS", func(t *testing.T) {
		plain := map[string]string{"websocket": `port: {{ .Ports.websocket }}`}
		if err := validateTLSSnippetRefs(plain, false); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// TestWriteServerTLSSnippetHandshakeFirst pins that handshake_first is rendered
// into the tls block only when requested.
func TestWriteServerTLSSnippetHandshakeFirst(t *testing.T) {
	base := tlsInstanceFiles{
		caPath:     "/tls/ca.pem",
		serverCert: "/tls/server.crt",
		serverKey:  "/tls/server.key",
		mutual:     true,
	}

	render := func(t *testing.T, files tlsInstanceFiles) string {
		t.Helper()
		dir := t.TempDir()
		rel, err := writeServerTLSSnippet(dir, &files)
		if err != nil {
			t.Fatalf("writeServerTLSSnippet: %v", err)
		}
		body, err := os.ReadFile(filepath.Join(dir, filepath.Base(rel)))
		if err != nil {
			t.Fatalf("read snippet: %v", err)
		}
		return string(body)
	}

	t.Run("present when set", func(t *testing.T) {
		files := base
		files.handshakeFirst = true
		if got := render(t, files); !strings.Contains(got, "handshake_first: true") {
			t.Fatalf("snippet missing handshake_first:\n%s", got)
		}
	})

	t.Run("absent when unset", func(t *testing.T) {
		if got := render(t, base); strings.Contains(got, "handshake_first") {
			t.Fatalf("snippet should not contain handshake_first:\n%s", got)
		}
	})
}

// TestRenderConfigClientAdvertiseWithTLS proves the operator's --advertise host
// is the single source of client_advertise even when the managed TLS snippet is
// included, and that an IPv6 advertise host renders as a valid bracketed
// host:port the NATS parser accepts.
func TestRenderConfigClientAdvertiseWithTLS(t *testing.T) {
	newTD := func() *templateData {
		td := defaultTemplateData()
		td.ServerName = "test"
		td.ClientPort = 14222
		td.StoreDir = t.TempDir()
		td.LogFile = filepath.Join(td.StoreDir, "server.log")
		td.TLSInclude = "snippets/_tls_managed.conf"
		return td
	}

	t.Run("advertise host is the only client_advertise even with managed TLS", func(t *testing.T) {
		td := newTD()
		td.AdvertiseHost = "myhost.example"

		out, err := renderConfig(td, serverConfigTemplate)
		if err != nil {
			t.Fatalf("renderConfig: %v", err)
		}
		got := string(out)

		if n := strings.Count(got, "client_advertise:"); n != 1 {
			t.Fatalf("want exactly one client_advertise, got %d:\n%s", n, got)
		}
		if !strings.Contains(got, `client_advertise: "myhost.example:14222"`) {
			t.Fatalf("rendered output missing advertise host:\n%s", got)
		}
	})

	t.Run("ipv6 advertise host is bracketed and parses", func(t *testing.T) {
		td := newTD()
		td.AdvertiseHost = "::1"

		out, err := renderConfig(td, serverConfigTemplate)
		if err != nil {
			t.Fatalf("renderConfig: %v", err)
		}
		if !strings.Contains(string(out), `client_advertise: "[::1]:14222"`) {
			t.Fatalf("ipv6 advertise host not bracketed:\n%s", out)
		}

		// Drop the managed-TLS include (its file/certs don't exist in this pure
		// render test) and confirm the parser accepts the bracketed value — a
		// bare "::1:14222" would be rejected.
		cfg := strings.ReplaceAll(string(out), `include "snippets/_tls_managed.conf"`, "")
		cfgPath := filepath.Join(t.TempDir(), "test.cfg")
		if err := os.WriteFile(cfgPath, []byte(cfg), 0600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		opts, err := server.ProcessConfigFile(cfgPath)
		if err != nil {
			t.Fatalf("ProcessConfigFile rejected bracketed client_advertise: %v", err)
		}
		if opts.ClientAdvertise != "[::1]:14222" {
			t.Fatalf("opts.ClientAdvertise = %q, want %q", opts.ClientAdvertise, "[::1]:14222")
		}
	})
}

// renderJS renders the built-in template with JetStream enabled and the given
// snippet map, returning the rendered config text.
func renderJS(t *testing.T, jetStream bool, snippets map[string]string) string {
	t.Helper()
	td := defaultTemplateData()
	td.ServerName = "n1"
	td.ClientPort = 14222
	td.StoreDir = "/tmp/store"
	td.JetStream = jetStream
	for k := range snippets {
		// Mirror what renderAndWriteSnippets records: the include path.
		td.Snippets[k] = "snippets/" + k + ".conf"
	}
	out, err := renderConfig(td, serverConfigTemplate)
	if err != nil {
		t.Fatalf("renderConfig: %v", err)
	}
	return string(out)
}

func TestRenderJetStreamSnippetIncludedInBlock(t *testing.T) {
	out := renderJS(t, true, map[string]string{"jetstream": "domain: HUB"})

	if !strings.Contains(out, `include "snippets/jetstream.conf"`) {
		t.Fatalf("rendered config missing jetstream include:\n---\n%s\n---", out)
	}

	// The include must sit inside the jetstream block: between "jetstream {"
	// and the next closing brace.
	start := strings.Index(out, "jetstream")
	if start < 0 {
		t.Fatalf("no jetstream block in output:\n%s", out)
	}
	block := out[start:]
	blockBody, _, found := strings.Cut(block, "}")
	if !found {
		t.Fatalf("jetstream block not closed:\n%s", block)
	}
	if !strings.Contains(blockBody, `include "snippets/jetstream.conf"`) {
		t.Fatalf("jetstream include is not inside the jetstream block:\n%s", blockBody)
	}
}

func TestRenderJetStreamNoSnippetHasNoInclude(t *testing.T) {
	out := renderJS(t, true, nil)
	if strings.Contains(out, "snippets/jetstream.conf") {
		t.Fatalf("unexpected jetstream include with no snippet:\n%s", out)
	}
	if !strings.Contains(out, "store_dir: /tmp/store") {
		t.Fatalf("expected built-in store_dir to remain:\n%s", out)
	}
}

// TestWriteServerTLSSnippetTimeout pins that the tls timeout renders the
// configured value, falling back to the managed default of 2 when unset or
// non-positive.
func TestWriteServerTLSSnippetTimeout(t *testing.T) {
	base := tlsInstanceFiles{
		caPath:     "/tls/ca.pem",
		serverCert: "/tls/server.crt",
		serverKey:  "/tls/server.key",
		mutual:     true,
	}

	render := func(t *testing.T, files tlsInstanceFiles) string {
		t.Helper()
		dir := t.TempDir()
		rel, err := writeServerTLSSnippet(dir, &files)
		if err != nil {
			t.Fatalf("writeServerTLSSnippet: %v", err)
		}
		body, err := os.ReadFile(filepath.Join(dir, filepath.Base(rel)))
		if err != nil {
			t.Fatalf("read snippet: %v", err)
		}
		return string(body)
	}

	t.Run("default when unset", func(t *testing.T) {
		if got := render(t, base); !strings.Contains(got, "timeout:   2") {
			t.Fatalf("snippet missing default timeout:\n%s", got)
		}
	})

	t.Run("fractional value", func(t *testing.T) {
		files := base
		files.timeoutSeconds = 0.5
		if got := render(t, files); !strings.Contains(got, "timeout:   0.5") {
			t.Fatalf("snippet missing timeout 0.5:\n%s", got)
		}
	})

	t.Run("sub-second value", func(t *testing.T) {
		files := base
		files.timeoutSeconds = 0.25
		if got := render(t, files); !strings.Contains(got, "timeout:   0.25") {
			t.Fatalf("snippet missing timeout 0.25:\n%s", got)
		}
	})

	t.Run("negative falls back to default", func(t *testing.T) {
		files := base
		files.timeoutSeconds = -1
		if got := render(t, files); !strings.Contains(got, "timeout:   2") {
			t.Fatalf("negative timeout should render default:\n%s", got)
		}
	})
}

func TestValidateSnippetKeysAllowsJetStream(t *testing.T) {
	if err := validateSnippetKeys(map[string]string{"jetstream": "domain: HUB"}); err != nil {
		t.Fatalf("jetstream should be an allowed snippet key, got: %v", err)
	}
}

func TestValidateJetStreamSnippet(t *testing.T) {
	// snippet present + JS off -> error
	if err := validateJetStreamSnippet(map[string]string{"jetstream": "domain: HUB"}, false); err == nil {
		t.Fatal("expected error when jetstream snippet set with JetStream disabled")
	}
	// empty snippet body + JS off -> still an error (key presence is what matters)
	if err := validateJetStreamSnippet(map[string]string{"jetstream": ""}, false); err == nil {
		t.Fatal("expected error for empty jetstream snippet with JetStream disabled")
	}
	// snippet present + JS on -> ok
	if err := validateJetStreamSnippet(map[string]string{"jetstream": "domain: HUB"}, true); err != nil {
		t.Fatalf("unexpected error with JetStream enabled: %v", err)
	}
	// no snippet + JS off -> ok
	if err := validateJetStreamSnippet(map[string]string{"accounts": "x"}, false); err != nil {
		t.Fatalf("unexpected error with no jetstream snippet: %v", err)
	}
	// nil snippets -> ok
	if err := validateJetStreamSnippet(nil, false); err != nil {
		t.Fatalf("unexpected error with nil snippets: %v", err)
	}
}

// mustRequest sends a request to the in-process management service and fails the
// test on a transport error or a Nats-Service-Error header, unmarshaling the body
// into resp when non-nil.
func mustRequest(t *testing.T, nc *nats.Conn, subject string, req any, resp any) {
	t.Helper()

	var payload []byte
	if req != nil {
		var err error
		payload, err = json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal %s: %v", subject, err)
		}
	}

	msg, err := nc.Request(subject, payload, 30*time.Second)
	if err != nil {
		t.Fatalf("request %s: %v", subject, err)
	}
	if e := msg.Header.Get("Nats-Service-Error"); e != "" {
		t.Fatalf("request %s failed: %s", subject, e)
	}
	if resp != nil {
		if err := json.Unmarshal(msg.Data, resp); err != nil {
			t.Fatalf("unmarshal %s response: %v", subject, err)
		}
	}
}

// TestCreateSuperCluster proves every node of every cluster starts. Each node binds
// a route and a gateway port reserved before it existed, so this exercises the
// hand-off of those reservations: a node whose listener was released early would
// fail to bind and fail the create.
func TestCreateSuperCluster(t *testing.T) {
	ms := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, ms.nc, "tester.create.super-cluster", api.CreateSuperClusterRequest{Clusters: 2, Servers: 2}, &created)

	if len(created.Servers) != 4 {
		t.Fatalf("created %d servers, want 4", len(created.Servers))
	}

	clusters := map[string]int{}
	for _, srv := range created.Servers {
		if !srv.Running {
			t.Errorf("server %q is not running", srv.Name)
		}
		if srv.Port == 0 {
			t.Errorf("server %q reported no client port", srv.Name)
		}
		clusters[srv.Cluster]++
	}
	if len(clusters) != 2 {
		t.Errorf("servers span %d clusters, want 2: %v", len(clusters), clusters)
	}
	for name, n := range clusters {
		if n != 2 {
			t.Errorf("cluster %q has %d servers, want 2", name, n)
		}
	}
}

// TestStatusReportsClientPort proves the status handler reports the client port
// (not the cluster/route port) so callers can build a usable connect URL.
func TestStatusReportsClientPort(t *testing.T) {
	ms := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, ms.nc, "tester.create.server", api.CreateServerRequest{}, &created)
	wantPort := created.Servers[0].Port
	if wantPort == 0 {
		t.Fatal("create response carried no client port")
	}

	var status api.StatusResponse
	mustRequest(t, ms.nc, "tester.status", api.StatusRequest{InstanceID: created.ID}, &status)
	if len(status.Instances) != 1 || len(status.Instances[0].Servers) != 1 {
		t.Fatalf("unexpected status shape: %+v", status)
	}
	if got := status.Instances[0].Servers[0].Port; got != wantPort {
		t.Fatalf("status Port = %d, want client port %d", got, wantPort)
	}
}

// TestStopStartInstance covers the instance-level lifecycle: a stop shuts every
// node down while keeping the persisted client port queryable, and a start revives
// them. Both are idempotent.
func TestStopStartInstance(t *testing.T) {
	ms := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, ms.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 3}, &created)
	id := created.ID

	var stopped api.StopInstanceResponse
	mustRequest(t, ms.nc, "tester.stop.instance", api.StopInstanceRequest{InstanceID: id}, &stopped)
	if len(stopped.Servers) != 3 {
		t.Fatalf("stop reported %d servers, want 3", len(stopped.Servers))
	}
	for _, s := range stopped.Servers {
		if s.Running {
			t.Errorf("server %q still running after stop", s.Name)
		}
		if s.Error != "" {
			t.Errorf("server %q stop error: %s", s.Name, s.Error)
		}
	}

	// The client port survives a stop, so status can still report it.
	var afterStop api.StatusResponse
	mustRequest(t, ms.nc, "tester.status", api.StatusRequest{InstanceID: id}, &afterStop)
	for _, s := range afterStop.Instances[0].Servers {
		if s.Running {
			t.Errorf("status shows server %q running after stop", s.Name)
		}
		if s.Port == 0 {
			t.Errorf("status lost client port for stopped server %q", s.Name)
		}
	}

	var started api.StartInstanceResponse
	mustRequest(t, ms.nc, "tester.start.instance", api.StartInstanceRequest{InstanceID: id}, &started)
	for _, s := range started.Servers {
		if !s.Running {
			t.Errorf("server %q not running after start: %s", s.Name, s.Error)
		}
	}

	// Idempotent: starting an already-running instance reports all running, no error.
	var startedAgain api.StartInstanceResponse
	mustRequest(t, ms.nc, "tester.start.instance", api.StartInstanceRequest{InstanceID: id}, &startedAgain)
	for _, s := range startedAgain.Servers {
		if !s.Running || s.Error != "" {
			t.Errorf("second start of %q: running=%v err=%q", s.Name, s.Running, s.Error)
		}
	}
}

// TestStopInstanceNotFound proves an unknown instance ID is rejected rather than
// silently succeeding.
func TestStopInstanceNotFound(t *testing.T) {
	ms := startTestService(t)

	payload, err := json.Marshal(api.StopInstanceRequest{InstanceID: "does-not-exist"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := ms.nc.Request("tester.stop.instance", payload, 10*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if msg.Header.Get("Nats-Service-Error") == "" {
		t.Fatal("expected a service error for an unknown instance ID")
	}
}
