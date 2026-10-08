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
	"crypto/tls"
	"flag"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// updateGolden rewrites the golden files under testdata/render from what the
// service writes, instead of comparing with them.
var updateGolden = flag.Bool("update", false, "rewrite the golden files under testdata/render")

// goldenPorts is the port range the golden tests hand out ports from. Only
// numbers inside it are treated as ports when the rendered files are normalized,
// so it must stay clear of every other number a config or snippet holds.
var goldenPorts = PortRange{Low: 27500, High: 27999}

// goldenSnippets are the snippets of the golden cases that use the built-in
// template: JetStream tuning plus the three snippets that reserve a listener,
// the websocket one wired to the generated TLS material.
var goldenSnippets = map[string]string{
	"jetstream": "max_memory_store: 64MB\n",
	"websocket": `websocket {
	port: {{ .Ports.websocket }}
	tls {
		cert_file: "{{ .TLS.CertFile }}"
		key_file: "{{ .TLS.KeyFile }}"
	}
}
`,
	"mqtt": `mqtt {
	port: {{ .Ports.mqtt }}
}
`,
	"leafnode": `leafnodes {
	port: {{ .Ports.leafnode }}
}
`,
}

// goldenCustomSnippets are the snippets of the custom template case, which has
// no generated TLS.
var goldenCustomSnippets = map[string]string{
	"top": `# top snippet of {{ .ServerName }} in {{ .ClusterName }}
max_connections: 1000
`,
	"websocket": `websocket {
	port: {{ .Ports.websocket }}
	no_tls: true
}
`,
	"leafnode": `leafnodes {
	port: {{ .Ports.leafnode }}
}
`,
}

// goldenCustomTemplate is the main template of the custom template case. Its
// comments print every template variable, and its config wires the cluster from
// .Routes.
var goldenCustomTemplate = `# custom template
# InstanceID {{ .InstanceID }} ShortID {{ .ShortID }} Kind {{ .Kind }} Description {{ .Description }}
# ServerName {{ .ServerName }} ServerIndex {{ .ServerIndex }} ClusterIndex {{ .ClusterIndex }} ClusterSize {{ .ClusterSize }}
# ServerDir {{ .ServerDir }} StoreDir {{ .StoreDir }} LogFile {{ .LogFile }}
# Host {{ .Host }} AdvertiseHost {{ .AdvertiseHost }} JetStream {{ .JetStream }} ClientPort {{ .ClientPort }}
# ClusterName {{ .ClusterName }} ClusterPort {{ .ClusterPort }} Routes {{ .Routes }}
# Clusters {{ .Clusters }} GatewayPort {{ .GatewayPort }} Gateways {{ .Gateways }}
# Ports {{ .Ports }} Snippets {{ .Snippets }} TLSInclude {{ .TLSInclude }} TLS {{ .TLS }}
{{ if .Snippets.top }}include "{{ .Snippets.top }}"{{ end }}
server_name: "{{ .ServerName }}"
listen: "{{ hostport .Host .ClientPort }}"
log_file: "{{ .LogFile }}"
cluster {
	name: "{{ .ClusterName }}"
	port: {{ .ClusterPort }}
	routes: [
{{- range .Routes }}
		"nats://{{ . }}"
{{- end }}
	]
}
{{ if .Snippets.websocket }}include "{{ .Snippets.websocket }}"{{ end }}
{{ if .Snippets.leafnode }}include "{{ .Snippets.leafnode }}"{{ end }}
`

// goldenCase is one create the golden tests render: the request sent to the
// service and the instance spec the planner is given for the same create.
type goldenCase struct {
	name    string
	subject string
	req     any
	spec    instanceSpec
}

// goldenCases are the creates whose rendered configs and snippets are kept as
// golden files under testdata/render.
func goldenCases() []goldenCase {
	return []goldenCase{
		{
			name:    "server",
			subject: "tester.create.server",
			req: api.CreateServerRequest{
				JetStream:   true,
				Description: "golden server",
				Snippets:    goldenSnippets,
				TLS:         &api.TLSOptions{Mode: api.TLSModeServer},
			},
			spec: instanceSpec{
				Kind:          "server",
				Servers:       1,
				JetStream:     true,
				Description:   "golden server",
				Snippets:      goldenSnippets,
				MainTemplate:  serverConfigTemplate,
				AdvertiseHost: "localhost",
				TLS:           true,
			},
		},
		{
			name:    "cluster",
			subject: "tester.create.cluster",
			req: api.CreateClusterRequest{
				Servers:     3,
				JetStream:   true,
				Description: "golden cluster",
				Snippets:    goldenSnippets,
				TLS: &api.TLSOptions{
					Mode:           api.TLSModeMutual,
					SANs:           []string{"node.example", "10.1.2.3"},
					HandshakeFirst: true,
				},
			},
			spec: instanceSpec{
				Kind:              "cluster",
				Servers:           3,
				JetStream:         true,
				Description:       "golden cluster",
				Snippets:          goldenSnippets,
				MainTemplate:      serverConfigTemplate,
				AdvertiseHost:     "localhost",
				TLS:               true,
				TLSMutual:         true,
				TLSSANs:           []string{"node.example", "10.1.2.3"},
				TLSHandshakeFirst: true,
			},
		},
		{
			name:    "super-cluster",
			subject: "tester.create.super-cluster",
			req: api.CreateSuperClusterRequest{
				Servers:     2,
				Clusters:    2,
				JetStream:   true,
				Description: "golden super-cluster",
				Snippets:    goldenSnippets,
				TLS: &api.TLSOptions{
					Mode:    api.TLSModeServer,
					Timeout: 3.5,
				},
			},
			spec: instanceSpec{
				Kind:          "super-cluster",
				Servers:       2,
				Clusters:      2,
				JetStream:     true,
				Description:   "golden super-cluster",
				Snippets:      goldenSnippets,
				MainTemplate:  serverConfigTemplate,
				AdvertiseHost: "localhost",
				TLS:           true,
				TLSTimeout:    3.5,
			},
		},
		{
			name:    "cluster-custom-template",
			subject: "tester.create.cluster",
			req: api.CreateClusterRequest{
				Servers:     2,
				Description: "golden custom template",
				Snippets:    goldenCustomSnippets,
				Template:    goldenCustomTemplate,
			},
			spec: instanceSpec{
				Kind:         "cluster",
				Servers:      2,
				Description:  "golden custom template",
				Snippets:     goldenCustomSnippets,
				MainTemplate: goldenCustomTemplate,
			},
		},
	}
}

// TestCreateRendersGoldenFiles creates each golden case through the service and
// compares every config and snippet it wrote with the case's golden file, after
// the instance id, short id, instance directory and ports are replaced with
// placeholders. The generated PEM files differ on every run, so they are parsed
// and their SANs and file modes checked instead.
func TestCreateRendersGoldenFiles(t *testing.T) {
	for _, tc := range goldenCases() {
		t.Run(tc.name, func(t *testing.T) {
			svc := startTestService(t, func(o *Options) {
				o.PortRange = goldenPorts
			})

			var created api.CreateResponse
			mustRequest(t, svc.nc, tc.subject, tc.req, &created)

			instDir := filepath.Join(svc.Dir(), created.ID)
			rendered, tlsFiles := readInstanceFiles(t, instDir)

			got := normalizeRendered(rendered, instDir, created.ID)
			if *updateGolden {
				writeGolden(t, tc.name, got)
			} else {
				compareGolden(t, tc.name, got)
			}

			if !tc.spec.TLS {
				if len(tlsFiles) != 0 {
					t.Errorf("instance without TLS wrote TLS files: %v", slices.Sorted(maps.Keys(tlsFiles)))
				}
				return
			}

			checkTLSFiles(t, tlsFiles, effectiveSANs(tc.spec.TLSSANs, tc.spec.AdvertiseHost))
		})
	}
}

// renderedFile is a file read back from an instance directory or taken from a
// plan: its contents and its mode.
type renderedFile struct {
	data []byte
	mode fs.FileMode
}

// readInstanceFiles reads the configs and snippets under instDir, keyed by their
// path relative to instDir with each node's config named <node dir>/config.cfg,
// and the files under tls/, keyed the same way. It ignores everything else a
// running server writes, such as its log and JetStream store.
func readInstanceFiles(t *testing.T, instDir string) (map[string][]byte, map[string]renderedFile) {
	t.Helper()

	rendered := map[string][]byte{}
	tlsFiles := map[string]renderedFile{}

	err := filepath.WalkDir(instDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(instDir, path)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")

		var key string
		switch {
		case len(parts) == 2 && parts[0] == "tls":
			key = rel
		case len(parts) == 2 && filepath.Ext(parts[1]) == ".cfg":
			key = filepath.Join(parts[0], "config.cfg")
		case len(parts) == 3 && parts[1] == "snippets" && filepath.Ext(parts[2]) == ".conf":
			key = rel
		default:
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		if parts[0] == "tls" {
			info, err := d.Info()
			if err != nil {
				return err
			}
			tlsFiles[key] = renderedFile{data: data, mode: info.Mode().Perm()}
			return nil
		}

		_, dup := rendered[key]
		if dup {
			t.Errorf("more than one file for %s", key)
		}
		rendered[key] = data

		return nil
	})
	if err != nil {
		t.Fatalf("read instance files: %v", err)
	}

	return rendered, tlsFiles
}

// digitRun matches a run of decimal digits, a candidate port.
var digitRun = regexp.MustCompile(`[0-9]+`)

// normalizeRendered joins the rendered files, in sorted path order, into one
// text with placeholders for everything that differs from one create to the
// next: the instance directory, the instance id, the short id and the ports.
// Every number inside goldenPorts is a port; ports are numbered by order of
// first appearance.
func normalizeRendered(files map[string][]byte, instDir, id string) string {
	ports := map[string]string{}
	var out strings.Builder

	for _, path := range slices.Sorted(maps.Keys(files)) {
		text := string(files[path])
		text = strings.ReplaceAll(text, instDir, "<INSTANCE_DIR>")
		text = strings.ReplaceAll(text, id, "<ID>")
		text = strings.ReplaceAll(text, shortID(id), "<SHORT>")
		text = digitRun.ReplaceAllStringFunc(text, func(digits string) string {
			n, err := strconv.Atoi(digits)
			if err != nil || n < goldenPorts.Low || n > goldenPorts.High {
				return digits
			}

			placeholder, seen := ports[digits]
			if !seen {
				placeholder = fmt.Sprintf("<PORT%d>", len(ports)+1)
				ports[digits] = placeholder
			}
			return placeholder
		})

		fmt.Fprintf(&out, "=== %s\n%s\n", filepath.ToSlash(path), text)
	}

	return out.String()
}

// goldenPath returns the path of the named golden file.
func goldenPath(name string) string {
	return filepath.Join("testdata", "render", name+".golden")
}

// writeGolden rewrites testdata/render/<name>.golden with got. Only the
// request-driven test writes golden files, so they always hold what the service
// writes.
func writeGolden(t *testing.T, name, got string) {
	t.Helper()

	path := goldenPath(name)
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		t.Fatalf("create golden dir: %v", err)
	}
	err = os.WriteFile(path, []byte(got), 0644)
	if err != nil {
		t.Fatalf("write golden file: %v", err)
	}
}

// compareGolden compares got with testdata/render/<name>.golden and fails at
// the first line that differs.
func compareGolden(t *testing.T, name, got string) {
	t.Helper()

	path := goldenPath(name)
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden file: %v", err)
	}
	if got == string(want) {
		return
	}

	gotLines := strings.Split(got, "\n")
	wantLines := strings.Split(string(want), "\n")
	for i := range max(len(gotLines), len(wantLines)) {
		var g, w string
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if g != w {
			t.Fatalf("rendered files differ from %s at line %d:\n got: %q\nwant: %q", path, i+1, g, w)
		}
	}
}

// checkTLSFiles checks the generated TLS files, keyed by their path relative to
// the instance directory: the CA, server certificate and server key are there
// with modes 0644, 0644 and 0600, the certificate is signed by the CA and
// carries exactly wantSANs, and the key belongs to the certificate.
func checkTLSFiles(t *testing.T, files map[string]renderedFile, wantSANs []string) {
	t.Helper()

	wantModes := map[string]fs.FileMode{
		filepath.Join("tls", "ca.pem"):     0644,
		filepath.Join("tls", "server.crt"): 0644,
		filepath.Join("tls", "server.key"): 0600,
	}

	got := slices.Sorted(maps.Keys(files))
	want := slices.Sorted(maps.Keys(wantModes))
	if !slices.Equal(got, want) {
		t.Fatalf("TLS files = %v, want %v", got, want)
	}
	for path, mode := range wantModes {
		if files[path].mode != mode {
			t.Errorf("%s mode = %o, want %o", path, files[path].mode, mode)
		}
	}

	ca := parseFirstCert(t, files[filepath.Join("tls", "ca.pem")].data)
	if !ca.IsCA {
		t.Errorf("ca.pem is not a CA certificate")
	}

	certPEM := files[filepath.Join("tls", "server.crt")].data
	cert := parseFirstCert(t, certPEM)
	err := cert.CheckSignatureFrom(ca)
	if err != nil {
		t.Errorf("server certificate is not signed by the CA: %v", err)
	}

	wantDNS, wantIPs := classifySANs(wantSANs)
	var gotIPs, wantIPStrings []string
	for _, ip := range cert.IPAddresses {
		gotIPs = append(gotIPs, ip.String())
	}
	for _, ip := range wantIPs {
		wantIPStrings = append(wantIPStrings, ip.String())
	}
	if !slices.Equal(cert.DNSNames, wantDNS) {
		t.Errorf("server certificate DNS SANs = %v, want %v", cert.DNSNames, wantDNS)
	}
	if !slices.Equal(gotIPs, wantIPStrings) {
		t.Errorf("server certificate IP SANs = %v, want %v", gotIPs, wantIPStrings)
	}

	_, err = tls.X509KeyPair(certPEM, files[filepath.Join("tls", "server.key")].data)
	if err != nil {
		t.Errorf("server key does not match the certificate: %v", err)
	}
}
