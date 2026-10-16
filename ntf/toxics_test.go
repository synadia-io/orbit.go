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

// TestPlanRefusesRouteTLS proves a plan with toxics fails with
// planErrRouteTLS, answered with 019, when a node's config enables TLS in its
// cluster block from the template or in its gateway block from a snippet, and
// that client TLS and a plan without toxics are not refused.
func TestPlanRefusesRouteTLS(t *testing.T) {
	tlsSnippet := map[string]string{"tls": "tls {\n\tcert_file: \"/certs/server.crt\"\n}\n"}

	tests := []struct {
		name string
		spec instanceSpec
		// block is the block the refusal names, empty when the plan succeeds.
		block string
	}{
		{
			name:  "cluster tls from the template",
			spec:  instanceSpec{Kind: "cluster", Servers: 2, MainTemplate: toxicsTLSTemplate(routeTLSBlock), Toxics: true},
			block: "cluster",
		},
		{
			name:  "gateway tls from a snippet",
			spec:  instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2, MainTemplate: toxicsTLSTemplate(""), Snippets: tlsSnippet, Toxics: true},
			block: "gateway",
		},
		{
			name: "client tls",
			spec: instanceSpec{Kind: "cluster", Servers: 2, MainTemplate: serverConfigTemplate, TLS: true, Toxics: true},
		},
		{
			name: "cluster tls without toxics",
			spec: instanceSpec{Kind: "cluster", Servers: 2, MainTemplate: toxicsTLSTemplate(routeTLSBlock)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := nuid.Next()

			_, err := planInstance(tt.spec, id, newFakePlacement(id), server.VERSION)
			if tt.block == "" {
				if err != nil {
					t.Fatalf("planInstance: %v", err)
				}
				return
			}

			if !errors.Is(err, errRouteTLS) {
				t.Fatalf("planInstance error %v does not wrap errRouteTLS", err)
			}
			if planErrorCode(err) != "019" {
				t.Errorf("error code = %s, want 019", planErrorCode(err))
			}
			want := "enables TLS in its " + tt.block + " block"
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not say %q", err, want)
			}
		})
	}
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
