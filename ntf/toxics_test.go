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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	"github.com/synadia-io/orbit.go/ntf/agent"
	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/backend"
)

// TestPlanProxies proves a plan with toxics asks for one proxy per directed
// route link, one per cluster and node of another cluster for the gateways,
// one per node's client listener and one dead advertise address for a cluster
// or super-cluster, after the same listener reservations as without toxics,
// and that a plan without toxics asks for no proxy at all. It then checks the
// shape of the proxies, the links dialing them and the template data.
func TestPlanProxies(t *testing.T) {
	tests := []struct {
		name string
		spec instanceSpec
		// kinds counts the proxy addresses asked for, by kind.
		kinds map[string]int
	}{
		{
			name:  "server",
			spec:  instanceSpec{Kind: "server", Servers: 1},
			kinds: map[string]int{"client": 1},
		},
		{
			name:  "cluster",
			spec:  instanceSpec{Kind: "cluster", Servers: 3},
			kinds: map[string]int{"route": 6, "client": 3, "advertise": 1},
		},
		{
			name:  "super-cluster",
			spec:  instanceSpec{Kind: "super-cluster", Servers: 3, Clusters: 2},
			kinds: map[string]int{"route": 12, "gateway": 6, "client": 6, "advertise": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.spec.MainTemplate = serverConfigTemplate
			id := nuid.Next()

			plain := newFakePlacement(id)
			plainPlan, err := planInstance(tt.spec, id, plain, server.VERSION)
			if err != nil {
				t.Fatalf("planInstance without toxics: %v", err)
			}
			if len(plain.proxyCalls) != 0 {
				t.Errorf("plan without toxics asked for proxies %v", plain.proxyCalls)
			}
			if plainPlan.proxies != nil || plainPlan.clientProxies != nil || plainPlan.deadAdvertise != (backend.Endpoint{}) {
				t.Errorf("plan without toxics has proxies")
			}
			for _, n := range plainPlan.nodes {
				td := n.TemplateData
				if td.ClientAdvertise != "" || td.ClusterAdvertise != "" || td.GatewayAdvertise != "" {
					t.Errorf("node %s renders advertise addresses without toxics", n.Name)
				}
			}

			tt.spec.Toxics = true
			place := newFakePlacement(id)
			plan, err := planInstance(tt.spec, id, place, server.VERSION)
			if err != nil {
				t.Fatalf("planInstance: %v", err)
			}

			if !slices.Equal(place.calls, plain.calls) {
				t.Errorf("listener addresses with toxics\n%v\nwant\n%v", place.calls, plain.calls)
			}

			kinds := map[string]int{}
			for _, call := range place.proxyCalls {
				kind, _, _ := strings.Cut(call, " ")
				kinds[kind]++
			}
			if !maps.Equal(kinds, tt.kinds) {
				t.Errorf("proxy addresses asked for by kind = %v, want %v", kinds, tt.kinds)
			}

			checkPlannedProxies(t, plan)
		})
	}
}

// checkPlannedProxies checks the proxies of plan, planned with toxics at a
// fakePlacement: each route proxy goes from a node to another of its cluster
// and forwards to that node's cluster listener, each gateway proxy goes from a
// cluster to a node of another cluster and forwards to its gateway listener,
// every route link and every gateway link to another cluster dials its proxy
// on 127.0.0.1, and each node renders its routes, gateways and advertise
// addresses from them, with its own cluster's .Gateways entries left as the
// direct gateway listeners.
func checkPlannedProxies(t *testing.T, plan *instancePlan) {
	t.Helper()

	byName := map[string]*nodePlan{}
	for _, n := range plan.nodes {
		byName[n.Name] = n
	}

	listen := map[string]backend.Endpoint{}
	for _, p := range plan.proxies {
		listen[p.Name] = p.Listen

		if p.Listen.Host != "127.0.0.1" {
			t.Errorf("proxy %s listens on %s, want 127.0.0.1", p.Name, p.Listen)
		}

		to := byName[p.To]
		switch p.Kind {
		case "route":
			if p.Name != routeLinkName(p.From, p.To) {
				t.Errorf("route proxy named %q from %s to %s", p.Name, p.From, p.To)
			}
			if byName[p.From].Cluster != to.Cluster || p.From == p.To {
				t.Errorf("route proxy %s does not join two nodes of a cluster", p.Name)
			}
			if p.Upstream != listenerOf(to, "cluster").Endpoint {
				t.Errorf("route proxy %s forwards to %s, want %s's cluster listener", p.Name, p.Upstream, p.To)
			}

		case "gateway":
			if p.Name != gatewayLinkName(p.From, p.To) {
				t.Errorf("gateway proxy named %q from %s to %s", p.Name, p.From, p.To)
			}
			if to.Cluster == p.From {
				t.Errorf("gateway proxy %s reaches a node of its own cluster", p.Name)
			}
			if p.Upstream != listenerOf(to, "gateway").Endpoint {
				t.Errorf("gateway proxy %s forwards to %s, want %s's gateway listener", p.Name, p.Upstream, p.To)
			}

		default:
			t.Errorf("proxy %s has kind %q", p.Name, p.Kind)
		}
	}

	for _, lk := range plan.Links {
		var want backend.Endpoint
		switch {
		case lk.Kind == "route":
			want = listen[routeLinkName(lk.Node, lk.Peer)]
		case byName[lk.Node].Cluster == byName[lk.Peer].Cluster:
			want = listenerOf(byName[lk.Peer], "gateway").Endpoint
		default:
			want = listen[gatewayLinkName(byName[lk.Node].Cluster, lk.Peer)]
		}
		if lk.Dial != want {
			t.Errorf("%s link from %s to %s dials %s, want %s", lk.Kind, lk.Node, lk.Peer, lk.Dial, want)
		}
	}

	dead := plan.deadAdvertise
	if plan.Kind == "server" {
		if dead != (backend.Endpoint{}) {
			t.Errorf("server has dead advertise address %s, want none", dead)
		}
	} else if dead.Host != "127.0.0.1" || dead.Port == 0 {
		t.Errorf("dead advertise address = %s, want a port on 127.0.0.1", dead)
	}

	for _, n := range plan.nodes {
		td := n.TemplateData
		config := string(n.Config)

		client, ok := plan.clientProxies[n.Name]
		if !ok || client.Host != "localhost" {
			t.Errorf("node %s client proxy = %s, want one on localhost", n.Name, client)
		}
		if td.ClientAdvertise != client.String() {
			t.Errorf("node %s .ClientAdvertise = %q, want %q", n.Name, td.ClientAdvertise, client)
		}
		if !strings.Contains(config, `client_advertise: "`+client.String()+`"`) {
			t.Errorf("node %s config does not advertise its client proxy %s", n.Name, client)
		}

		if n.Cluster == "" {
			if td.ClusterAdvertise != "" || td.GatewayAdvertise != "" {
				t.Errorf("server renders cluster or gateway advertise addresses")
			}
			continue
		}

		if td.ClusterAdvertise != dead.String() {
			t.Errorf("node %s .ClusterAdvertise = %q, want %q", n.Name, td.ClusterAdvertise, dead)
		}
		if !strings.Contains(config, `advertise: "`+dead.String()+`"`) {
			t.Errorf("node %s config does not advertise the dead address %s", n.Name, dead)
		}

		var wantRoutes []string
		for _, peer := range nodesOf(plan, n.Cluster) {
			if peer == n {
				wantRoutes = append(wantRoutes, listenerOf(n, "cluster").Endpoint.String())
				continue
			}
			wantRoutes = append(wantRoutes, listen[routeLinkName(n.Name, peer.Name)].String())
		}
		if !slices.Equal(td.Routes, wantRoutes) {
			t.Errorf("node %s .Routes = %v, want %v", n.Name, td.Routes, wantRoutes)
		}

		if plan.Kind != "super-cluster" {
			if td.GatewayAdvertise != "" {
				t.Errorf("node %s renders a gateway advertise address in a cluster", n.Name)
			}
			continue
		}

		if td.GatewayAdvertise != dead.String() {
			t.Errorf("node %s .GatewayAdvertise = %q, want %q", n.Name, td.GatewayAdvertise, dead)
		}
		for _, cluster := range plan.Clusters {
			var want []string
			for _, peer := range nodesOf(plan, cluster) {
				// A node's own cluster keeps the direct gateway listeners,
				// whose URLs nats-server ignores.
				if cluster == n.Cluster {
					want = append(want, listenerOf(peer, "gateway").Endpoint.String())
					continue
				}
				want = append(want, listen[gatewayLinkName(n.Cluster, peer.Name)].String())
			}
			if !slices.Equal(td.Gateways[cluster], want) {
				t.Errorf("node %s .Gateways[%s] = %v, want %v", n.Name, cluster, td.Gateways[cluster], want)
			}
		}
	}
}

// TestPlanProxiesClientAdvertiseHost proves a node with an advertise host
// advertises its client proxy's port on that host.
func TestPlanProxiesClientAdvertiseHost(t *testing.T) {
	id := nuid.Next()
	spec := instanceSpec{Kind: "cluster", Servers: 2, MainTemplate: serverConfigTemplate, AdvertiseHost: "ci.example", Toxics: true}

	plan, err := planInstance(spec, id, newFakePlacement(id), server.VERSION)
	if err != nil {
		t.Fatalf("planInstance: %v", err)
	}

	for _, n := range plan.nodes {
		want := backend.Endpoint{Host: "ci.example", Port: plan.clientProxies[n.Name].Port}.String()
		if n.TemplateData.ClientAdvertise != want {
			t.Errorf("node %s .ClientAdvertise = %q, want %q", n.Name, n.TemplateData.ClientAdvertise, want)
		}
	}
}

// toxicsTLSTemplate is a custom template that references every variable of
// toxics and puts the tls snippet inside the gateway block. clusterTLS, when
// set, is put inside the cluster block.
func toxicsTLSTemplate(clusterTLS string) string {
	return `server_name: "{{ .ServerName }}"
port: {{ .ClientPort }}
client_advertise: "{{ .ClientAdvertise }}"
cluster {
	name: "{{ .ClusterName }}"
	port: {{ .ClusterPort }}
	advertise: "{{ .ClusterAdvertise }}"
	routes: [
{{- range .Routes }}
		"nats://{{ . }}"
{{- end }}
	]
` + clusterTLS + `
}
{{ if .GatewayPort }}
gateway {
	name: "{{ .ClusterName }}"
	port: {{ .GatewayPort }}
	advertise: "{{ .GatewayAdvertise }}"
	# {{ .Gateways }}
	{{ if .Snippets.tls }}include "{{ .Snippets.tls }}"{{ end }}
}
{{ end }}
`
}

// routeTLSBlock is a tls block a test puts inside a cluster block.
const routeTLSBlock = `	tls {
		cert_file: "/certs/server.crt"
		key_file: "/certs/server.key"
	}`

// inlineTLSInclude includes the tls snippet after another field on the same
// line, where an include does not start its line.
const inlineTLSInclude = "\tconnect_retries: 3; {{ if .Snippets.tls }}include \"{{ .Snippets.tls }}\"{{ end }}"

// emptyDir fails t when dir holds anything.
func emptyDir(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, e := range entries {
		t.Errorf("%s left behind in %s", e.Name(), dir)
	}
}

// TestToxicsConfigRefusal proves the checks of an instance created with
// toxics, run on each planned node's config as the server reads it, refuse
// TLS in a cluster or gateway block or a gateway entry with 019, a gateway not
// named for its cluster and a config that does not parse with 007, wherever
// an include puts what they look for, pass a valid config with its includes,
// and leave no temporary directory behind.
func TestToxicsConfigRefusal(t *testing.T) {
	id := nuid.Next()
	short := shortID(id)
	tlsSnippet := map[string]string{"tls": "tls {\n\tcert_file: \"/certs/server.crt\"\n}\n"}
	routeTLS := func(node, block string) string {
		return fmt.Sprintf("%s: node %s enables TLS in its %s block", errRouteTLS, node, block)
	}

	tests := []struct {
		name string
		spec instanceSpec
		// code and want are the refusal, both empty when nothing is refused.
		code string
		want string
	}{
		{
			name: "cluster tls from the template",
			spec: instanceSpec{Kind: "cluster", Servers: 2, MainTemplate: toxicsTLSTemplate(routeTLSBlock), Toxics: true},
			code: "019",
			want: routeTLS(short+"-n1", "cluster"),
		},
		{
			name: "gateway tls from a snippet",
			spec: instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2, MainTemplate: toxicsTLSTemplate(""), Snippets: tlsSnippet, Toxics: true},
			code: "019",
			want: routeTLS(short+"-c1_s1", "gateway"),
		},
		{
			name: "cluster tls from an inline include",
			spec: instanceSpec{Kind: "cluster", Servers: 2, MainTemplate: toxicsTLSTemplate(inlineTLSInclude), Snippets: tlsSnippet, Toxics: true},
			code: "019",
			want: routeTLS(short+"-n1", "cluster"),
		},
		{
			name: "cluster after a brace that ends an include",
			spec: instanceSpec{
				Kind:         "cluster",
				Servers:      2,
				MainTemplate: toxicsTLSTemplate("") + "\ninclude \"{{ .Snippets.top }}\"}cluster { tls { cert_file: \"/certs/server.crt\" } }\n",
				Snippets:     map[string]string{"top": "max_payload: 1024\n"},
				Toxics:       true,
			},
			code: "019",
			want: routeTLS(short+"-n1", "cluster"),
		},
		{
			name: "tls after a brace inside an included snippet",
			spec: instanceSpec{
				Kind:         "cluster",
				Servers:      2,
				MainTemplate: toxicsTLSTemplate("\tinclude \"{{ .Snippets.tls }}\""),
				Snippets:     map[string]string{"tls": "x: 1}tls { cert_file: \"/certs/server.crt\" }\n"},
				Toxics:       true,
			},
			code: "019",
			want: routeTLS(short+"-n1", "cluster"),
		},
		{
			name: "gateway entry tls",
			spec: instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2, MainTemplate: gatewayEntryTLSTemplate(t), Toxics: true},
			code: "019",
			want: fmt.Sprintf("%s: node %s-c1_s1 enables TLS in its gateway entry SC_%s_2", errRouteTLS, short, short),
		},
		{
			name: "other gateway name",
			spec: instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2, MainTemplate: gatewayOtherNameTemplate(t), Toxics: true},
			code: "007",
			want: fmt.Sprintf(`%s: node %s-c1_s1 names its gateway "elsewhere", want "SC_%s_1"`, errGatewayName, short, short),
		},
		{
			name: "no gateway name",
			spec: instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2, MainTemplate: gatewayNoNameTemplate(t), Toxics: true},
			code: "007",
			want: fmt.Sprintf(`%s: node %s-c1_s1 does not name its gateway, want "SC_%s_1"`, errGatewayName, short, short),
		},
		{
			name: "unparseable config",
			spec: instanceSpec{Kind: "cluster", Servers: 2, MainTemplate: serverConfigTemplate + "\nbroken {\n", Toxics: true},
			code: "007",
			want: fmt.Sprintf("%s: node %s-n1: ", errConfigParse, short),
		},
		{
			name: "valid config with includes and client tls",
			spec: instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2, MainTemplate: serverConfigTemplate, Snippets: map[string]string{"top": "max_payload: 1024\n"}, TLS: true, Toxics: true},
		},
		{
			name: "cluster with its own gateway block",
			spec: instanceSpec{Kind: "cluster", Servers: 2, MainTemplate: serverConfigTemplate + "\ngateway {\n\tname: \"elsewhere\"\n\tport: -1\n}\n", Toxics: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := planInstance(tt.spec, id, newFakePlacement(id), server.VERSION)
			if err != nil {
				t.Fatalf("planInstance: %v", err)
			}

			dir := t.TempDir()
			var code, msg string
			for _, n := range plan.nodes {
				code, msg = toxicsConfigRefusal(dir, n, "002")
				if code != "" {
					break
				}
			}
			emptyDir(t, dir)

			if code != tt.code {
				t.Errorf("code = %q (%s), want %q", code, msg, tt.code)
			}
			if !strings.HasPrefix(msg, tt.want) {
				t.Errorf("message %q does not start with %q", msg, tt.want)
			}
		})
	}
}

// TestReadNodeConfig proves readNodeConfig reads a node's snippets and its
// managed TLS snippet through their includes, removes its temporary
// directory, and refuses a file path that leaves that directory.
func TestReadNodeConfig(t *testing.T) {
	id := nuid.Next()
	spec := instanceSpec{Kind: "cluster", Servers: 2, MainTemplate: serverConfigTemplate, Snippets: map[string]string{"top": "max_payload: 1024\n"}, TLS: true}

	plan, err := planInstance(spec, id, newFakePlacement(id), server.VERSION)
	if err != nil {
		t.Fatalf("planInstance: %v", err)
	}
	n := plan.nodes[0]

	dir := t.TempDir()
	cfg, err := readNodeConfig(dir, n)
	if err != nil {
		t.Fatalf("readNodeConfig: %v", err)
	}
	emptyDir(t, dir)

	if cfg["max_payload"] != int64(1024) {
		t.Errorf("max_payload = %v, want 1024 from the top snippet", cfg["max_payload"])
	}
	_, ok := cfg["tls"]
	if !ok {
		t.Error("config has no tls block from the managed TLS snippet")
	}

	escaping := &nodePlan{NodePlan: n.NodePlan, TLS: n.TLS}
	escaping.Files = []backend.File{{Path: filepath.Join("..", "outside.conf"), Data: []byte("x: 1\n")}}
	_, err = readNodeConfig(dir, escaping)
	if !errors.Is(err, errConfigParse) {
		t.Errorf("readNodeConfig of a file outside the directory: %v, want errConfigParse", err)
	}
	emptyDir(t, dir)
}

// TestReadNodeConfigErrorPaths proves a parse error of readNodeConfig names
// files in the instance directory, never in its removed temporary directory,
// for a missing include and a syntax error, and that an absolute include
// resolves under the including file's directory, as conf joins it, not at
// its path on disk.
func TestReadNodeConfigErrorPaths(t *testing.T) {
	id := nuid.Next()
	place := newFakePlacement(id)

	outside := filepath.Join(t.TempDir(), "outside.conf")
	err := os.WriteFile(outside, []byte("max_payload: 7\n"), 0600)
	if err != nil {
		t.Fatalf("write %s: %v", outside, err)
	}

	tests := []struct {
		name  string
		extra string
		// want is what the message says, empty when only the path is checked.
		want string
	}{
		{"missing include", "\ninclude \"snippets/missing.conf\"\n", filepath.Join(place.InstanceDir(), "n1", "snippets", "missing.conf")},
		{"syntax error", "\nbroken {\n", ""},
		{"absolute include", "\ninclude \"" + outside + "\"\n", filepath.Join(place.InstanceDir(), "n1", outside)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := instanceSpec{Kind: "cluster", Servers: 2, MainTemplate: serverConfigTemplate + tt.extra}
			plan, err := planInstance(spec, id, place, server.VERSION)
			if err != nil {
				t.Fatalf("planInstance: %v", err)
			}

			dir := t.TempDir()
			_, err = readNodeConfig(dir, plan.nodes[0])
			emptyDir(t, dir)

			if !errors.Is(err, errConfigParse) {
				t.Fatalf("readNodeConfig: %v, want errConfigParse", err)
			}
			if strings.Contains(err.Error(), "config-check-") {
				t.Errorf("error %q names the temporary directory", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not name %q", err, tt.want)
			}
		})
	}
}

// gatewayNameLine and gatewayEntryLine are lines of the built-in template's
// gateway block a test replaces: the gateway name, and the start of each
// entry of the gateways list.
const (
	gatewayNameLine  = "\tname: \"{{ .ClusterName }}\"\n\tport: {{ .GatewayPort }}"
	gatewayEntryLine = `{ name: "{{ $cluster }}", urls: [`
)

// gatewayTemplate returns the built-in template with from, a line of its
// gateway block, replaced by to.
func gatewayTemplate(t *testing.T, from, to string) string {
	t.Helper()

	if strings.Count(serverConfigTemplate, from) != 1 {
		t.Fatalf("the built-in template does not hold %q once", from)
	}

	return strings.Replace(serverConfigTemplate, from, to, 1)
}

// gatewayEntryTLSTemplate puts TLS, keyed in upper case, in the entry of the
// gateways list for a super-cluster's second cluster.
func gatewayEntryTLSTemplate(t *testing.T) string {
	t.Helper()

	return gatewayTemplate(t, gatewayEntryLine, `{ name: "{{ $cluster }}", {{ if eq $cluster (index $.Clusters 1) }}TLS: { cert_file: "/certs/server.crt" }, {{ end }}urls: [`)
}

// gatewayOtherNameTemplate names the gateway "elsewhere", keyed in mixed case.
func gatewayOtherNameTemplate(t *testing.T) string {
	t.Helper()

	return gatewayTemplate(t, gatewayNameLine, "\tName: \"elsewhere\"\n\tport: {{ .GatewayPort }}")
}

// gatewayNoNameTemplate leaves the name out of the gateway block.
func gatewayNoNameTemplate(t *testing.T) string {
	t.Helper()

	return gatewayTemplate(t, gatewayNameLine, "\tport: {{ .GatewayPort }}")
}

// TestInProcessPlacementProxyAddress proves the in-process placement holds a
// route or client proxy's port until the service releases it, on 127.0.0.1
// and localhost, never holds the dead advertise port, which refuses a dial,
// keeps every proxy port reserved until teardown, and closes and releases them
// all on teardown.
func TestInProcessPlacementProxyAddress(t *testing.T) {
	s := placementService(t, freeRange(t, 4))
	inst := s.newInstance("cluster", "")
	p := placeInstance(t, s, inst)

	route, err := p.ProxyAddress("route:a->b", "route")
	if err != nil {
		t.Fatalf("ProxyAddress route: %v", err)
	}
	client, err := p.ProxyAddress("client:a", "client")
	if err != nil {
		t.Fatalf("ProxyAddress client: %v", err)
	}
	dead, err := p.ProxyAddress("advertise", "advertise")
	if err != nil {
		t.Fatalf("ProxyAddress advertise: %v", err)
	}

	if route.Host != "127.0.0.1" || client.Host != "localhost" || dead.Host != "127.0.0.1" {
		t.Errorf("hosts = %s, %s and %s, want 127.0.0.1, localhost and 127.0.0.1", route.Host, client.Host, dead.Host)
	}
	if portBindable(route.Port) || portBindable(client.Port) {
		t.Errorf("route or client proxy port not held")
	}

	conn, err := net.DialTimeout("tcp", dead.String(), 2*time.Second)
	if err == nil {
		conn.Close()
		t.Errorf("dial to the dead advertise address %s succeeded", dead)
	}

	s.backend.(*inProcessBackend).releaseProxies(inst.ID, []string{"route:a->b"})

	if !portBindable(route.Port) {
		t.Errorf("released route proxy port %d still held", route.Port)
	}
	if portBindable(client.Port) {
		t.Errorf("client proxy port %d released with the route proxy", client.Port)
	}
	for _, port := range []int{route.Port, client.Port, dead.Port} {
		if portFree(s.ports, port) {
			t.Errorf("proxy port %d no longer reserved before teardown", port)
		}
	}

	s.dropInstance(inst)

	if !portBindable(client.Port) {
		t.Errorf("client proxy port %d still held after teardown", client.Port)
	}
	if trackedPorts(s) != 0 {
		t.Errorf("%d ports still reserved after teardown", trackedPorts(s))
	}
}

// serviceError sends a request expected to fail and returns the service error
// code and message.
func serviceError(t *testing.T, nc *nats.Conn, subject string, req any) (string, string) {
	t.Helper()

	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := nc.Request(subject, payload, 30*time.Second)
	if err != nil {
		t.Fatalf("request %s: %v", subject, err)
	}

	return msg.Header.Get("Nats-Service-Error-Code"), msg.Header.Get("Nats-Service-Error")
}

// TestToxicsWithoutToxicsAgentIsRefused proves every kind of create refuses
// toxics with 014 when no agent is selected and with 018, naming the agent and
// the capability, when the agent does not declare toxics, and never starts the
// agent.
func TestToxicsWithoutToxicsAgentIsRefused(t *testing.T) {
	plain := startTestService(t)
	code, _ := serviceError(t, plain.nc, "tester.create.server", api.CreateServerRequest{Toxics: true})
	if code != "014" {
		t.Errorf("error code without an agent = %q, want 014", code)
	}

	provider := &fakeProvider{}
	svc := startTestService(t, withAgent(provider))

	tests := []struct {
		name    string
		subject string
		req     any
	}{
		{"server", "tester.create.server", api.CreateServerRequest{Toxics: true}},
		{"cluster", "tester.create.cluster", api.CreateClusterRequest{Servers: 3, Toxics: true}},
		{"super-cluster", "tester.create.super-cluster", api.CreateSuperClusterRequest{Clusters: 2, Servers: 3, Toxics: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, msg := serviceError(t, svc.nc, tt.subject, tt.req)
			if code != "018" {
				t.Errorf("error code = %q, want 018", code)
			}
			if !strings.Contains(msg, `"fake"`) || !strings.Contains(msg, `"toxics"`) {
				t.Errorf("error %q does not name the agent and the capability", msg)
			}
		})
	}

	if provider.startCount() != 0 {
		t.Errorf("provider started %d agents, want none", provider.startCount())
	}
}

// TestToxicsCustomTemplateIsRefused proves a create with toxics refuses a
// custom template with 007 unless it references the variables that carry the
// proxies for its kind, naming only those it lacks.
func TestToxicsCustomTemplateIsRefused(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	tests := []struct {
		name    string
		subject string
		req     any
		missing string
	}{
		{
			name:    "server",
			subject: "tester.create.server",
			req:     api.CreateServerRequest{Template: "port: {{ .ClientPort }}", Toxics: true},
			missing: ".ClientAdvertise when",
		},
		{
			name:    "cluster",
			subject: "tester.create.cluster",
			req:     api.CreateClusterRequest{Servers: 2, Template: "port: {{ .ClientPort }} {{ .Routes }}", Toxics: true},
			missing: "reference .ClusterAdvertise, .ClientAdvertise when",
		},
		{
			name:    "super-cluster",
			subject: "tester.create.super-cluster",
			req:     api.CreateSuperClusterRequest{Clusters: 2, Servers: 2, Template: "{{ .Routes }} {{ .ClusterAdvertise }} {{ .ClientAdvertise }}", Toxics: true},
			missing: "reference .Gateways, .GatewayAdvertise when",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, msg := serviceError(t, svc.nc, tt.subject, tt.req)
			if code != "007" {
				t.Errorf("error code = %q, want 007", code)
			}
			if !strings.Contains(msg, tt.missing) {
				t.Errorf("error %q does not name exactly what is missing, %q", msg, tt.missing)
			}
		})
	}

	if provider.startCount() != 0 {
		t.Errorf("provider started %d agents, want none", provider.startCount())
	}
}

// TestToxicsRouteTLSIsRefused proves a create with toxics whose config enables
// TLS on its routes is refused with 019, before the agent starts, and leaves
// no port reserved.
func TestToxicsRouteTLSIsRefused(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	code, msg := serviceError(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 2, Template: toxicsTLSTemplate(routeTLSBlock), Toxics: true})
	if code != "019" {
		t.Errorf("error code = %q, want 019", code)
	}
	if !strings.Contains(msg, errRouteTLS.Error()) || !strings.Contains(msg, "cluster block") {
		t.Errorf("error %q does not explain the refusal", msg)
	}
	if provider.startCount() != 0 {
		t.Errorf("provider started %d agents, want none", provider.startCount())
	}
	if trackedPorts(svc) != 0 {
		t.Errorf("%d ports still reserved after the refusal", trackedPorts(svc))
	}
}

// TestToxicsCreateReportsLinks proves a cluster created with toxics hands the
// agent its route proxies and client proxies, with the route and gateway
// holds released before Start and the client holds released after it, lists
// every link in the create response and in status, reports each server's
// client proxy as its port with the live client port as the upstream, and
// advertises a dead address that refuses a dial and stays reserved.
func TestToxicsCreateReportsLinks(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 3, Toxics: true}, &created)

	specs, _ := provider.snapshot()
	if len(specs) != 1 {
		t.Fatalf("provider saw %d specs, want 1", len(specs))
	}
	spec := specs[0]
	if len(spec.Links) != 6 || len(spec.ClientProxies) != 3 {
		t.Fatalf("spec has %d links and %d client proxies, want 6 and 3", len(spec.Links), len(spec.ClientProxies))
	}

	if len(created.Links) != 9 {
		t.Fatalf("create response has %d links, want 9", len(created.Links))
	}
	byName := map[string]api.Link{}
	for i, lk := range created.Links {
		byName[lk.Name] = lk
		wantKind := "route"
		if i >= 6 {
			wantKind = "client"
		}
		if lk.Kind != wantKind {
			t.Errorf("link %d %s has kind %q, want %q", i, lk.Name, lk.Kind, wantKind)
		}
	}

	for _, sl := range spec.Links {
		if sl.Kind != "route" || sl.Name != "route:"+sl.From+"->"+sl.To {
			t.Errorf("spec link %+v is not a route link named for its nodes", sl)
		}
		want := api.Link{Name: sl.Name, Kind: "route", From: sl.From, To: sl.To, Proxy: sl.Listen.String(), Upstream: sl.Upstream.String()}
		if byName[sl.Name] != want {
			t.Errorf("link %s = %+v, want %+v", sl.Name, byName[sl.Name], want)
		}
	}

	for _, srv := range created.Servers {
		proxy, ok := spec.ClientProxies[srv.Name]
		if !ok {
			t.Fatalf("spec has no client proxy for %s", srv.Name)
		}
		if srv.Port != proxy.Port {
			t.Errorf("server %s port = %d, want its client proxy's %d", srv.Name, srv.Port, proxy.Port)
		}

		var live backend.Endpoint
		for _, a := range svc.findServerByName(srv.Name).backendNode.Addresses() {
			if a.Name == "client" {
				live = a.Internal
			}
		}

		lk := byName["client:"+srv.Name]
		want := api.Link{Name: "client:" + srv.Name, Kind: "client", To: srv.Name, Proxy: proxy.String(), Upstream: live.String()}
		if lk != want {
			t.Errorf("client link = %+v, want %+v", lk, want)
		}

		conn, err := net.DialTimeout("tcp", lk.Upstream, 2*time.Second)
		if err != nil {
			t.Errorf("server %s live client port %s does not answer: %v", srv.Name, lk.Upstream, err)
			continue
		}
		conn.Close()
	}

	provider.mu.Lock()
	bindable := maps.Clone(provider.bindable)
	provider.mu.Unlock()
	for _, sl := range spec.Links {
		if !bindable[sl.Listen.Port] {
			t.Errorf("route proxy %s port still held at Start", sl.Name)
		}
	}
	for name, ep := range spec.ClientProxies {
		if bindable[ep.Port] {
			t.Errorf("client proxy of %s released before Start", name)
		}
		if !portBindable(ep.Port) {
			t.Errorf("client proxy of %s still held after the create", name)
		}
		if portFree(svc.ports, ep.Port) {
			t.Errorf("client proxy port of %s no longer reserved", name)
		}
	}

	svc.mu.Lock()
	dead := svc.instances[created.ID].plan.deadAdvertise
	svc.mu.Unlock()
	conn, err := net.DialTimeout("tcp", dead.String(), 2*time.Second)
	if err == nil {
		conn.Close()
		t.Errorf("dial to the dead advertise address %s succeeded", dead)
	}
	if portFree(svc.ports, dead.Port) {
		t.Errorf("dead advertise port %d no longer reserved", dead.Port)
	}

	var status api.StatusResponse
	mustRequest(t, svc.nc, "tester.status", api.StatusRequest{InstanceID: created.ID}, &status)
	if len(status.Instances) != 1 {
		t.Fatalf("status returned %d instances, want 1", len(status.Instances))
	}
	if !reflect.DeepEqual(status.Instances[0].Links, created.Links) {
		t.Errorf("status links = %+v, want the create response's %+v", status.Instances[0].Links, created.Links)
	}
	for i, srv := range status.Instances[0].Servers {
		if srv.Port != created.Servers[i].Port {
			t.Errorf("status port of %s = %d, want %d", srv.Name, srv.Port, created.Servers[i].Port)
		}
	}
}

// TestToxicsSuperClusterLinks proves a super-cluster created with toxics lists
// its route links, then a gateway link from each cluster to each node of the
// other cluster, then its client links, and hands the agent its route and
// gateway proxies.
func TestToxicsSuperClusterLinks(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.super-cluster", api.CreateSuperClusterRequest{Clusters: 2, Servers: 2, Toxics: true}, &created)

	cluster := map[string]string{}
	for _, srv := range created.Servers {
		cluster[srv.Name] = srv.Cluster
	}

	var kinds []string
	for _, lk := range created.Links {
		if len(kinds) == 0 || kinds[len(kinds)-1] != lk.Kind {
			kinds = append(kinds, lk.Kind)
		}
		if lk.Kind != "gateway" {
			continue
		}
		if lk.Name != "gateway:"+lk.From+"->"+lk.To {
			t.Errorf("gateway link %q is not named for its cluster and node", lk.Name)
		}
		if cluster[lk.To] == lk.From || cluster[lk.To] == "" {
			t.Errorf("gateway link %s does not reach a node of another cluster", lk.Name)
		}
	}
	if !slices.Equal(kinds, []string{"route", "gateway", "client"}) {
		t.Errorf("links are grouped by kind as %v, want route, gateway, client", kinds)
	}
	if len(created.Links) != 12 {
		t.Errorf("create response has %d links, want 4 route, 4 gateway and 4 client", len(created.Links))
	}

	specs, _ := provider.snapshot()
	if len(specs) != 1 || len(specs[0].Links) != 8 {
		t.Fatalf("agent was handed %d specs, want one with the 8 route and gateway links", len(specs))
	}
}

// TestToxicsUpdate proves an update of a server created with toxics refuses a
// custom template that does not reference the variables that carry the
// proxies, and that an update with the built-in template renders the same
// proxy addresses without reserving another port.
func TestToxicsUpdate(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 2, Toxics: true}, &created)

	name := created.Servers[0].Name
	cfgPath := svc.findServerByName(name).backendNode.(*inProcessNode).cfgPath
	before, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	reserved := trackedPorts(svc)

	code, msg := serviceError(t, svc.nc, "tester.update.server", api.UpdateServerRequest{Name: name, Template: "port: {{ .ClientPort }} {{ .Routes }}"})
	if code != "007" {
		t.Errorf("error code = %q, want 007", code)
	}
	if !strings.Contains(msg, "reference .ClusterAdvertise, .ClientAdvertise when") {
		t.Errorf("error %q does not name exactly what is missing", msg)
	}

	var updated api.UpdateServerResponse
	mustRequest(t, svc.nc, "tester.update.server", api.UpdateServerRequest{Name: name}, &updated)

	after, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Errorf("update with the built-in template changed the config\n%s\nwant\n%s", after, before)
	}
	if trackedPorts(svc) != reserved {
		t.Errorf("update reserved %d ports, want %d", trackedPorts(svc), reserved)
	}
}

// TestToxicsUpdateRouteTLSIsRefused proves an update of a server created with
// toxics is refused with 019, and writes nothing, when its template or a
// snippet puts TLS in the cluster block.
func TestToxicsUpdateRouteTLSIsRefused(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 2, Toxics: true}, &created)

	name := created.Servers[0].Name
	cfgPath := svc.findServerByName(name).backendNode.(*inProcessNode).cfgPath
	before, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}

	snippetInCluster := "\t{{ if .Snippets.tls }}include \"{{ .Snippets.tls }}\"{{ end }}"

	tests := []struct {
		name string
		req  api.UpdateServerRequest
	}{
		{"template", api.UpdateServerRequest{Name: name, Template: toxicsTLSTemplate(routeTLSBlock)}},
		{"snippet", api.UpdateServerRequest{Name: name, Template: toxicsTLSTemplate(snippetInCluster), Snippets: map[string]string{"tls": "tls {\n\tcert_file: \"/certs/server.crt\"\n}\n"}}},
		{"inline snippet", api.UpdateServerRequest{Name: name, Template: toxicsTLSTemplate(inlineTLSInclude), Snippets: map[string]string{"tls": "tls {\n\tcert_file: \"/certs/server.crt\"\n}\n"}}},
		{"brace after an include", api.UpdateServerRequest{Name: name, Template: toxicsTLSTemplate("") + "\ninclude \"{{ .Snippets.top }}\"}cluster { tls { cert_file: \"/certs/server.crt\" } }\n", Snippets: map[string]string{"top": "max_payload: 1024\n"}}},
		{"brace inside a snippet", api.UpdateServerRequest{Name: name, Template: toxicsTLSTemplate("\tinclude \"{{ .Snippets.tls }}\""), Snippets: map[string]string{"tls": "x: 1}tls { cert_file: \"/certs/server.crt\" }\n"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, msg := serviceError(t, svc.nc, "tester.update.server", tt.req)
			if code != "019" {
				t.Errorf("error code = %q, want 019", code)
			}
			if !strings.Contains(msg, errRouteTLS.Error()) || !strings.Contains(msg, "cluster block") {
				t.Errorf("error %q does not explain the refusal", msg)
			}
		})
	}

	after, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Errorf("a refused update changed the config\n%s\nwant\n%s", after, before)
	}
}

// TestToxicsGatewayRefusals proves a create of a super-cluster with toxics is
// refused, before the agent starts and leaving no port reserved, with 019 when
// an entry of its gateways list enables TLS and with 007 when its gateway has
// no name or a name other than its cluster's.
func TestToxicsGatewayRefusals(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	tests := []struct {
		name     string
		template string
		code     string
		want     []string
	}{
		{"gateway entry tls", gatewayEntryTLSTemplate(t), "019", []string{errRouteTLS.Error(), "enables TLS in its gateway entry SC_"}},
		{"other gateway name", gatewayOtherNameTemplate(t), "007", []string{errGatewayName.Error(), `names its gateway "elsewhere", want "SC_`}},
		{"no gateway name", gatewayNoNameTemplate(t), "007", []string{errGatewayName.Error(), `does not name its gateway, want "SC_`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, msg := serviceError(t, svc.nc, "tester.create.super-cluster", api.CreateSuperClusterRequest{Clusters: 2, Servers: 2, Template: tt.template, Toxics: true})
			if code != tt.code {
				t.Errorf("error code = %q, want %s", code, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q does not say %q", msg, want)
				}
			}
		})
	}

	if provider.startCount() != 0 {
		t.Errorf("provider started %d agents, want none", provider.startCount())
	}
	if trackedPorts(svc) != 0 {
		t.Errorf("%d ports still reserved after the refusals", trackedPorts(svc))
	}
	noConfigCheckDirs(t, svc)
}

// noConfigCheckDirs fails t when a temporary directory of readNodeConfig is
// left in svc's directory.
func noConfigCheckDirs(t *testing.T, svc *Service) {
	t.Helper()

	left, err := filepath.Glob(filepath.Join(svc.dir, "config-check-*"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(left) != 0 {
		t.Errorf("config check directories left behind: %v", left)
	}
}

// TestNoToxicsSkipsConfigChecks proves a create without toxics does not run
// the config checks: a config that does not parse fails when the server
// starts, not with the checks' 007.
func TestNoToxicsSkipsConfigChecks(t *testing.T) {
	svc := startTestService(t)

	code, msg := serviceError(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 2, Template: serverConfigTemplate + "\nbroken {\n"})
	if code == "" {
		t.Fatal("create of a config that does not parse succeeded")
	}
	if strings.Contains(msg, errConfigParse.Error()) {
		t.Errorf("create without toxics was refused by the config checks: %s %s", code, msg)
	}
}

// TestToxicsUpdateGatewayRefusals proves an update of a server of a
// super-cluster created with toxics is refused, and writes nothing, with 019
// when an entry of its gateways list enables TLS, naming the entry, and with
// 007 when its gateway has no name or a name other than its cluster's.
func TestToxicsUpdateGatewayRefusals(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.super-cluster", api.CreateSuperClusterRequest{Clusters: 2, Servers: 2, Toxics: true}, &created)

	// The entry TLS template puts TLS in the second cluster's entry, so the
	// server is picked from the first.
	svc.mu.Lock()
	clusters := slices.Clone(svc.instances[created.ID].plan.Clusters)
	svc.mu.Unlock()
	if len(clusters) != 2 {
		t.Fatalf("plan has %d clusters, want 2", len(clusters))
	}
	other := clusters[1]

	var srv *api.ManagedServer
	for _, s := range created.Servers {
		if s.Cluster == clusters[0] {
			srv = s
			break
		}
	}
	if srv == nil {
		t.Fatalf("no server in cluster %s", clusters[0])
	}

	cfgPath := svc.findServerByName(srv.Name).backendNode.(*inProcessNode).cfgPath
	before, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}

	tests := []struct {
		name     string
		template string
		code     string
		want     string
	}{
		{"gateway entry tls", gatewayEntryTLSTemplate(t), "019", fmt.Sprintf("%s: node %s enables TLS in its gateway entry %s", errRouteTLS, srv.Name, other)},
		{"other gateway name", gatewayOtherNameTemplate(t), "007", fmt.Sprintf(`%s: node %s names its gateway "elsewhere", want %q`, errGatewayName, srv.Name, srv.Cluster)},
		{"no gateway name", gatewayNoNameTemplate(t), "007", fmt.Sprintf(`%s: node %s does not name its gateway, want %q`, errGatewayName, srv.Name, srv.Cluster)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, msg := serviceError(t, svc.nc, "tester.update.server", api.UpdateServerRequest{Name: srv.Name, Template: tt.template})
			if code != tt.code {
				t.Errorf("error code = %q, want %s", code, tt.code)
			}
			if msg != tt.want {
				t.Errorf("error %q, want %q", msg, tt.want)
			}
		})
	}

	after, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Errorf("a refused update changed the config\n%s\nwant\n%s", after, before)
	}
}

// TestToxicsUnparseableConfigIsRefused proves a create with toxics whose
// config does not parse is refused with 007, naming the node, before the
// agent starts, and that an update of a server created with toxics is refused
// the same way and writes nothing.
func TestToxicsUnparseableConfigIsRefused(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))
	broken := serverConfigTemplate + "\nbroken {\n"

	code, msg := serviceError(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 2, Template: broken, Toxics: true})
	if code != "007" {
		t.Errorf("create error code = %q, want 007", code)
	}
	if !strings.HasPrefix(msg, errConfigParse.Error()+": node ") {
		t.Errorf("create error %q does not explain the refusal", msg)
	}
	if provider.startCount() != 0 {
		t.Errorf("provider started %d agents, want none", provider.startCount())
	}

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 2, Toxics: true}, &created)

	name := created.Servers[0].Name
	cfgPath := svc.findServerByName(name).backendNode.(*inProcessNode).cfgPath
	before, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}

	code, msg = serviceError(t, svc.nc, "tester.update.server", api.UpdateServerRequest{Name: name, Template: broken})
	if code != "007" {
		t.Errorf("update error code = %q, want 007", code)
	}
	if !strings.HasPrefix(msg, fmt.Sprintf("%s: node %s: ", errConfigParse, name)) {
		t.Errorf("update error %q does not explain the refusal", msg)
	}

	after, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Errorf("a refused update changed the config\n%s\nwant\n%s", after, before)
	}
	noConfigCheckDirs(t, svc)
}

// TestToxicsUpdateManagedTLSIsRefused proves an update of a server of a
// super-cluster created with toxics and generated TLS is refused with 019, and
// writes nothing, when its template includes the managed TLS snippet the
// create wrote inside the cluster or gateway block, though the update does
// not rewrite that snippet.
func TestToxicsUpdateManagedTLSIsRefused(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.super-cluster", api.CreateSuperClusterRequest{Clusters: 2, Servers: 2, TLS: &api.TLSOptions{Mode: api.TLSModeServer}, Toxics: true}, &created)

	name := created.Servers[0].Name
	cfgPath := svc.findServerByName(name).backendNode.(*inProcessNode).cfgPath
	before, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}

	managedInclude := "\t{{ if .TLSInclude }}include \"{{ .TLSInclude }}\"{{ end }}"

	tests := []struct {
		name     string
		template string
		block    string
	}{
		{"cluster", toxicsTLSTemplate(managedInclude), "cluster"},
		{"gateway", gatewayTemplate(t, gatewayNameLine, gatewayNameLine+"\n"+managedInclude), "gateway"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, msg := serviceError(t, svc.nc, "tester.update.server", api.UpdateServerRequest{Name: name, Template: tt.template})
			if code != "019" {
				t.Errorf("error code = %q, want 019", code)
			}
			want := fmt.Sprintf("%s: node %s enables TLS in its %s block", errRouteTLS, name, tt.block)
			if msg != want {
				t.Errorf("error %q, want %q", msg, want)
			}
		})
	}

	after, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Errorf("a refused update changed the config\n%s\nwant\n%s", after, before)
	}
	noConfigCheckDirs(t, svc)
}

// TestToxicsAloneHasNoShapingProxy proves an instance created with toxics and
// neither trace nor proxy answers the shaping endpoints with 015 and has
// status read no shaping sets, though its agent shapes, and that teardown
// still stops that agent.
func TestToxicsAloneHasNoShapingProxy(t *testing.T) {
	shaper := &fakeAgent{}
	provider := &fakeProvider{toxics: true, agent: shaper}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 2, Toxics: true}, &created)

	code, _ := serviceError(t, svc.nc, "tester.shape.set", api.ShapeSetRequest{InstanceID: created.ID, Set: exampleShapingSet()})
	if code != "015" {
		t.Errorf("shape set error code = %q, want 015", code)
	}
	code, _ = serviceError(t, svc.nc, "tester.shape.report", api.ShapeReportRequest{InstanceID: created.ID})
	if code != "015" {
		t.Errorf("shape report error code = %q, want 015", code)
	}

	var status api.StatusResponse
	mustRequest(t, svc.nc, "tester.status", api.StatusRequest{InstanceID: created.ID}, &status)
	if len(status.Instances) != 1 || len(status.Instances[0].ShapingSets) != 0 {
		t.Errorf("status = %+v, want one instance without shaping sets", status.Instances)
	}

	shaper.mu.Lock()
	setsCalls, sets := shaper.setsCalls, len(shaper.sets)
	shaper.mu.Unlock()
	if setsCalls != 0 || sets != 0 {
		t.Errorf("agent was asked for shaping sets %d times and given %d sets, want neither", setsCalls, sets)
	}

	mustRequest(t, svc.nc, "tester.destroy", api.DestroyRequest{InstanceID: created.ID}, nil)
	if shaper.stopCount() != 1 {
		t.Errorf("agent stopped %d times on teardown, want 1", shaper.stopCount())
	}
}

// TestToxicsClientLinkOnAdvertiseHost proves a client link reports the proxy on
// the advertise host, where clients dial it, while the agent is handed the
// endpoint placement returned.
func TestToxicsClientLinkOnAdvertiseHost(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider), func(o *Options) {
		o.AdvertiseHost = "127.0.0.1"
	})

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Toxics: true}, &created)

	specs, _ := provider.snapshot()
	if len(specs) != 1 {
		t.Fatalf("provider saw %d specs, want 1", len(specs))
	}

	srv := created.Servers[0]
	proxy := specs[0].ClientProxies[srv.Name]
	if proxy.Host != "localhost" || proxy.Port != srv.Port {
		t.Errorf("agent's client proxy = %s, want localhost on port %d", proxy, srv.Port)
	}

	want := backend.Endpoint{Host: "127.0.0.1", Port: proxy.Port}.String()
	if len(created.Links) != 1 || created.Links[0].Proxy != want {
		t.Errorf("links = %+v, want one client link dialed on %s", created.Links, want)
	}
}

// TestToxicsCreateRetryUsesFreshProxies proves a create with toxics whose agent
// could not listen runs again on fresh proxy ports and a fresh dead advertise
// address, and releases every port of the failed attempt.
func TestToxicsCreateRetryUsesFreshProxies(t *testing.T) {
	var svc *Service
	var first []int

	provider := &fakeProvider{toxics: true, startErrs: []error{fmt.Errorf("%w: taken", agent.ErrListen)}}
	provider.startHook = func(agent.Spec) {
		if first != nil {
			return
		}
		svc.ports.mu.Lock()
		first = slices.Sorted(maps.Keys(svc.ports.inUse))
		svc.ports.mu.Unlock()
	}
	svc = startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 2, Toxics: true}, &created)

	if provider.startCount() != 2 {
		t.Fatalf("agent started %d times, want 2", provider.startCount())
	}

	provider.mu.Lock()
	failed := slices.Clone(first)
	provider.mu.Unlock()
	if len(failed) == 0 {
		t.Fatal("no ports were reserved at the first Start")
	}

	svc.mu.Lock()
	inst := svc.instances[created.ID]
	current := slices.Clone(inst.ports)
	dead := inst.plan.deadAdvertise
	svc.mu.Unlock()

	if trackedPorts(svc) != len(current) {
		t.Errorf("%d ports reserved, want only the %d of the instance", trackedPorts(svc), len(current))
	}
	for _, port := range failed {
		if slices.Contains(current, port) {
			t.Errorf("port %d of the failed attempt reused", port)
		}
	}
	if slices.Contains(failed, dead.Port) || !slices.Contains(current, dead.Port) {
		t.Errorf("dead advertise port %d is not a fresh reservation of the instance", dead.Port)
	}

	specs, _ := provider.snapshot()
	if len(specs) != 1 {
		t.Fatalf("provider saw %d successful specs, want 1", len(specs))
	}
	for _, lk := range specs[0].Links {
		if slices.Contains(failed, lk.Listen.Port) {
			t.Errorf("route proxy %s reuses port %d of the failed attempt", lk.Name, lk.Listen.Port)
		}
	}
	for name, ep := range specs[0].ClientProxies {
		if slices.Contains(failed, ep.Port) {
			t.Errorf("client proxy of %s reuses port %d of the failed attempt", name, ep.Port)
		}
	}
}

// TestFrontsByOptIn proves which nodes a cluster create fronts, and with what:
// under toxics every node, in order, with its live client endpoint and Capture
// set only on the first node of a traced or proxied instance; without toxics
// only the first node of a traced one. Only a captured node gets a trace port,
// and the one agent is stopped once on teardown.
func TestFrontsByOptIn(t *testing.T) {
	tests := []struct {
		name    string
		req     api.CreateClusterRequest
		fronted int
		capture bool
	}{
		{"toxics only", api.CreateClusterRequest{Servers: 3, Toxics: true}, 3, false},
		{"toxics and proxy", api.CreateClusterRequest{Servers: 3, Toxics: true, Proxy: true}, 3, true},
		{"toxics and trace", api.CreateClusterRequest{Servers: 3, Toxics: true, Trace: true}, 3, true},
		{"trace without toxics", api.CreateClusterRequest{Servers: 3, Trace: true}, 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &fakeProvider{toxics: true}
			svc := startTestService(t, withAgent(provider))

			var created api.CreateResponse
			mustRequest(t, svc.nc, "tester.create.cluster", tt.req, &created)

			fronts, started := provider.frontSnapshot()
			if len(started) != 1 {
				t.Fatalf("provider started %d agents, want 1", len(started))
			}
			if len(fronts) != tt.fronted {
				t.Fatalf("agent was fronted %d times, want %d", len(fronts), tt.fronted)
			}

			for i, front := range fronts {
				srv := created.Servers[i]
				if front.ServerName != srv.Name {
					t.Errorf("front %d is %q, want %q", i, front.ServerName, srv.Name)
				}

				var live backend.Endpoint
				for _, a := range svc.findServerByName(srv.Name).backendNode.Addresses() {
					if a.Name == "client" {
						live = a.Internal
					}
				}
				if front.Client != live {
					t.Errorf("front of %s dials %s, want its live client %s", srv.Name, front.Client, live)
				}

				wantCapture := tt.capture && i == 0
				if front.Capture != wantCapture {
					t.Errorf("front of %s has Capture %v, want %v", srv.Name, front.Capture, wantCapture)
				}
			}

			for i, srv := range created.Servers {
				_, traced := srv.Ports["trace"]
				wantTraced := tt.capture && i == 0
				if traced != wantTraced {
					t.Errorf("server %s has a trace port: %v, want %v", srv.Name, traced, wantTraced)
				}
			}

			mustRequest(t, svc.nc, "tester.destroy", api.DestroyRequest{InstanceID: created.ID}, nil)
			if started[0].stopCount() != 1 {
				t.Errorf("agent stopped %d times on teardown, want 1", started[0].stopCount())
			}
		})
	}
}
