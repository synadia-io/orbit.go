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
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nats-io/nuid"
)

// fakePlacement places an instance at fixed ports and directories without
// touching the host. Ports are handed out one after another from the bottom of
// goldenPorts, and directories live under /ntf-plan/<id>.
type fakePlacement struct {
	id   string
	next int

	// calls records each listenerAddress call as <node dir>/<listener name>.
	calls []string

	// failAt makes the listenerAddress call with this 1-based number fail with
	// an error wrapping ErrPortRangeExhausted. Zero never fails.
	failAt int

	// hold makes every address come with a real listener on 127.0.0.1, kept in
	// held, standing in for a reserved port.
	hold bool
	held []*net.TCPListener
}

func newFakePlacement(id string) *fakePlacement {
	return &fakePlacement{id: id, next: goldenPorts.Low}
}

func (p *fakePlacement) listenerAddress(node, name string) (listener, *net.TCPListener, error) {
	p.calls = append(p.calls, strings.TrimPrefix(node, shortID(p.id)+"-")+"/"+name)
	if len(p.calls) == p.failAt {
		return listener{}, nil, fmt.Errorf("%w: fake", ErrPortRangeExhausted)
	}

	l := listener{Name: name, Host: "localhost", Port: p.next}
	p.next++

	if !p.hold {
		return l, nil, nil
	}

	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return listener{}, nil, err
	}
	p.held = append(p.held, ln)

	return l, ln, nil
}

func (p *fakePlacement) instanceDir() string {
	return filepath.Join("/ntf-plan", p.id)
}

func (p *fakePlacement) nodeDir(node string) string {
	return filepath.Join(p.instanceDir(), strings.TrimPrefix(node, shortID(p.id)+"-"))
}

var _ placement = (*fakePlacement)(nil)

// TestPlanInstanceMatchesGoldenFiles plans each golden case at a fake placement
// and compares the rendered configs and snippets with the golden files the
// service's own writes produced, after the same placeholder substitution. It
// checks the file modes and the generated TLS material too.
func TestPlanInstanceMatchesGoldenFiles(t *testing.T) {
	for _, tc := range goldenCases() {
		t.Run(tc.name, func(t *testing.T) {
			id := nuid.Next()
			place := newFakePlacement(id)

			plan, err := planInstance(tc.spec, id, place)
			if err != nil {
				t.Fatalf("planInstance: %v", err)
			}

			rendered := map[string][]byte{}
			for _, n := range plan.Nodes {
				dir, err := filepath.Rel(place.instanceDir(), place.nodeDir(n.Name))
				if err != nil {
					t.Fatalf("node dir: %v", err)
				}
				rendered[filepath.Join(dir, "config.cfg")] = n.Config

				for _, f := range n.Files {
					if f.Mode != 0600 {
						t.Errorf("%s mode = %o, want 600", f.Path, f.Mode)
					}
					rendered[f.Path] = f.Data
				}
			}

			compareGolden(t, tc.name, normalizeRendered(rendered, place.instanceDir(), id))

			if !tc.spec.TLS {
				if len(plan.Files) != 0 || plan.TLS != nil {
					t.Errorf("plan without TLS has instance files %v and TLS material %v", plan.Files, plan.TLS)
				}
				return
			}

			tlsFiles := map[string]renderedFile{}
			for _, f := range plan.Files {
				tlsFiles[f.Path] = renderedFile{data: f.Data, mode: f.Mode}
			}
			checkTLSFiles(t, tlsFiles, effectiveSANs(tc.spec.TLSSANs, tc.spec.AdvertiseHost))

			if plan.TLS == nil {
				t.Fatal("plan has no TLS material for the create response")
			}
			if plan.TLS.CAPEM != string(tlsFiles[filepath.Join("tls", "ca.pem")].data) {
				t.Error("create response CA differs from tls/ca.pem")
			}
			hasClient := plan.TLS.ClientCertPEM != "" && plan.TLS.ClientKeyPEM != ""
			if hasClient != tc.spec.TLSMutual {
				t.Errorf("create response has a client certificate: %v, want %v", hasClient, tc.spec.TLSMutual)
			}
		})
	}
}

// TestPlanInstanceReservationOrder proves the planner asks for listener
// addresses in the order the create handlers have always reserved ports, and
// leaves each node holding its own listeners.
func TestPlanInstanceReservationOrder(t *testing.T) {
	snippets := map[string]string{"websocket": "", "leafnode": "", "top": ""}
	node := func(dir string) []string {
		return []string{dir + "/client", dir + "/websocket", dir + "/leafnode"}
	}

	tests := []struct {
		name string
		spec instanceSpec
		want []string
	}{
		{
			name: "server",
			spec: instanceSpec{Kind: "server", Servers: 1},
			want: node("n1"),
		},
		{
			name: "cluster",
			spec: instanceSpec{Kind: "cluster", Servers: 3},
			want: slices.Concat(
				[]string{"n1/cluster", "n2/cluster", "n3/cluster"},
				node("n1"), node("n2"), node("n3"),
			),
		},
		{
			name: "super-cluster",
			spec: instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2},
			want: slices.Concat(
				[]string{"c1_s1/gateway", "c1_s2/gateway", "c2_s1/gateway", "c2_s2/gateway"},
				[]string{"c1_s1/cluster", "c1_s2/cluster"}, node("c1_s1"), node("c1_s2"),
				[]string{"c2_s1/cluster", "c2_s2/cluster"}, node("c2_s1"), node("c2_s2"),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.spec.Snippets = snippets
			tt.spec.MainTemplate = serverConfigTemplate

			id := nuid.Next()
			place := newFakePlacement(id)
			place.hold = true
			defer closeListeners(place.held)

			plan, err := planInstance(tt.spec, id, place)
			if err != nil {
				t.Fatalf("planInstance: %v", err)
			}

			if !slices.Equal(place.calls, tt.want) {
				t.Errorf("listener addresses asked for in order\n%v\nwant\n%v", place.calls, tt.want)
			}

			var held int
			for _, n := range plan.Nodes {
				if len(n.Held) != len(n.Listeners) {
					t.Errorf("node %s holds %d listeners for %d listeners", n.Name, len(n.Held), len(n.Listeners))
				}
				held += len(n.Held)
			}
			if held != len(place.held) {
				t.Errorf("nodes hold %d listeners, placement handed out %d", held, len(place.held))
			}
		})
	}
}

// TestPlanInstanceNodesAndLinks proves the planner names the clusters and nodes
// as the create handlers do, links every node to each route and gateway peer
// but never to itself, dials each peer's own listener, and records the node the
// capture proxy fronts.
func TestPlanInstanceNodesAndLinks(t *testing.T) {
	id := nuid.Next()
	short := shortID(id)
	place := newFakePlacement(id)
	spec := instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2, MainTemplate: serverConfigTemplate, Proxy: true}

	plan, err := planInstance(spec, id, place)
	if err != nil {
		t.Fatalf("planInstance: %v", err)
	}

	wantClusters := []string{"SC_" + short + "_1", "SC_" + short + "_2"}
	if !slices.Equal(plan.Clusters, wantClusters) {
		t.Errorf("clusters = %v, want %v", plan.Clusters, wantClusters)
	}

	var names []string
	for _, n := range plan.Nodes {
		names = append(names, n.Name)
		wantCluster := wantClusters[n.ClusterIndex-1]
		if n.Cluster != wantCluster {
			t.Errorf("node %s cluster = %q, want %q", n.Name, n.Cluster, wantCluster)
		}
		if n.TemplateData == nil || n.TemplateData.ServerName != n.Name {
			t.Errorf("node %s has no template data of its own", n.Name)
		}
	}
	wantNames := []string{short + "-c1_s1", short + "-c1_s2", short + "-c2_s1", short + "-c2_s2"}
	if !slices.Equal(names, wantNames) {
		t.Errorf("nodes = %v, want %v", names, wantNames)
	}

	byName := map[string]*nodePlan{}
	for _, n := range plan.Nodes {
		byName[n.Name] = n
	}

	routes := map[string]int{}
	gateways := map[string]int{}
	for _, lk := range plan.Links {
		if lk.Node == lk.Peer {
			t.Errorf("node %s links to itself", lk.Node)
		}

		peer := byName[lk.Peer]
		var want listener
		switch lk.Kind {
		case "route":
			routes[lk.Node]++
			want = listenerOf(peer, "cluster")
			if peer.Cluster != byName[lk.Node].Cluster {
				t.Errorf("route link from %s to %s crosses clusters", lk.Node, lk.Peer)
			}
		case "gateway":
			gateways[lk.Node]++
			want = listenerOf(peer, "gateway")
		default:
			t.Errorf("link from %s to %s has kind %q", lk.Node, lk.Peer, lk.Kind)
		}

		if lk.Host != want.Host || lk.Port != want.Port {
			t.Errorf("%s link from %s to %s dials %s:%d, want %s:%d", lk.Kind, lk.Node, lk.Peer, lk.Host, lk.Port, want.Host, want.Port)
		}
	}
	for _, name := range wantNames {
		if routes[name] != 1 || gateways[name] != 3 {
			t.Errorf("node %s has %d route and %d gateway links, want 1 and 3", name, routes[name], gateways[name])
		}
	}

	if plan.ProxyNode != wantNames[0] {
		t.Errorf("proxy node = %q, want %q", plan.ProxyNode, wantNames[0])
	}

	spec.Proxy = false
	plan, err = planInstance(spec, id, newFakePlacement(id))
	if err != nil {
		t.Fatalf("planInstance without the proxy: %v", err)
	}
	if plan.ProxyNode != "" {
		t.Errorf("proxy node = %q without the proxy, want none", plan.ProxyNode)
	}
}

// TestPlanInstanceErrors proves each kind of planning failure is returned with
// the message the create handlers answer it with, and that every listener the
// placement handed out is closed again.
func TestPlanInstanceErrors(t *testing.T) {
	tests := []struct {
		name string
		spec instanceSpec
		// failAt is the listenerAddress call that fails, 0 for none.
		failAt int
		kind   planErrorKind
		prefix string
		// is is a cause the error must wrap, nil for none.
		is error
	}{
		{
			// A DNS SAN outside ASCII cannot be encoded in a certificate.
			name:   "TLS setup",
			spec:   instanceSpec{Kind: "server", Servers: 1, TLS: true, TLSSANs: []string{"bad" + string(rune(0xe9)) + ".example"}},
			kind:   planErrTLSSetup,
			prefix: "TLS setup failed: tls material: ",
		},
		{
			name:   "cluster port",
			spec:   instanceSpec{Kind: "cluster", Servers: 3},
			failAt: 3,
			kind:   planErrPort,
			prefix: "could not get free port: ",
			is:     ErrPortRangeExhausted,
		},
		{
			name:   "client port",
			spec:   instanceSpec{Kind: "cluster", Servers: 3},
			failAt: 6,
			kind:   planErrPort,
			prefix: "could not get free port: ",
			is:     errNoFreePort,
		},
		{
			name:   "snippet listener port",
			spec:   instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2, Snippets: map[string]string{"websocket": "", "mqtt": ""}},
			failAt: 9,
			kind:   planErrPort,
			prefix: `listener "mqtt": `,
			is:     ErrPortRangeExhausted,
		},
		{
			name:   "snippet",
			spec:   instanceSpec{Kind: "cluster", Servers: 2, Snippets: map[string]string{"top": "{{ .ServerName "}},
			kind:   planErrSnippet,
			prefix: `Snippet failure: snippet "top" parse: `,
		},
		{
			name:   "template on a later node",
			spec:   instanceSpec{Kind: "cluster", Servers: 3, MainTemplate: "{{ if eq .ServerIndex 2 }}{{ .NoSuchField }}{{ end }}"},
			kind:   planErrTemplate,
			prefix: "Template parse failure: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.spec.MainTemplate == "" {
				tt.spec.MainTemplate = serverConfigTemplate
			}

			id := nuid.Next()
			place := newFakePlacement(id)
			place.hold = true
			place.failAt = tt.failAt

			plan, err := planInstance(tt.spec, id, place)
			if err == nil {
				closeListeners(place.held)
				t.Fatal("planInstance succeeded, want an error")
			}
			if plan != nil {
				t.Errorf("planInstance returned a plan with its error")
			}

			var perr *planError
			if !errors.As(err, &perr) {
				t.Fatalf("error %v is not a *planError", err)
			}
			if perr.Kind != tt.kind {
				t.Errorf("error kind = %d, want %d", perr.Kind, tt.kind)
			}
			if !strings.HasPrefix(err.Error(), tt.prefix) {
				t.Errorf("error %q does not start with %q", err, tt.prefix)
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Errorf("error %v does not wrap %v", err, tt.is)
			}

			if tt.failAt > 0 && len(place.held) != tt.failAt-1 {
				t.Errorf("placement handed out %d listeners, want %d", len(place.held), tt.failAt-1)
			}
			for i, ln := range place.held {
				err := ln.Close()
				if !errors.Is(err, net.ErrClosed) {
					t.Errorf("listener %d was left open after the failure", i+1)
				}
			}
		})
	}
}
