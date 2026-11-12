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
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nuid"

	"github.com/synadia-io/orbit.go/ntf/backend"
)

// fakePlacement places an instance at fixed ports and directories without
// touching the host. Ports are handed out one after another from the bottom of
// goldenPorts, and directories live under /ntf-plan/<id>.
type fakePlacement struct {
	id   string
	next int

	// calls records each ListenerAddress call as <node dir>/<listener name>.
	calls []string

	// proxyCalls records each ProxyAddress call as <kind> <name>.
	proxyCalls []string

	// failAt makes the ListenerAddress or ProxyAddress call with this 1-based
	// number, counting both, fail with an error wrapping
	// ErrPortRangeExhausted. Zero never fails.
	failAt int
}

func newFakePlacement(id string) *fakePlacement {
	return &fakePlacement{id: id, next: goldenPorts.Low}
}

func (p *fakePlacement) ListenerAddress(node, name string) (backend.Listener, error) {
	p.calls = append(p.calls, node+"/"+name)
	if len(p.calls) == p.failAt {
		return backend.Listener{}, fmt.Errorf("%w: fake", ErrPortRangeExhausted)
	}

	l := backend.Listener{Name: name, Endpoint: backend.Endpoint{Host: "localhost", Port: p.next}}
	p.next++

	return l, nil
}

// ProxyAddress hands out the next port as the in-process placement does: on
// 127.0.0.1, or on localhost for a client proxy.
func (p *fakePlacement) ProxyAddress(name, kind string) (backend.Endpoint, error) {
	p.proxyCalls = append(p.proxyCalls, kind+" "+name)
	if len(p.calls)+len(p.proxyCalls) == p.failAt {
		return backend.Endpoint{}, fmt.Errorf("%w: fake", ErrPortRangeExhausted)
	}

	ep := backend.Endpoint{Host: "127.0.0.1", Port: p.next}
	if kind == "client" {
		ep.Host = "localhost"
	}
	p.next++

	return ep, nil
}

func (p *fakePlacement) InstanceDir() string {
	return filepath.Join("/ntf-plan", p.id)
}

func (p *fakePlacement) StoreDir(dir string) string {
	return filepath.Join(p.InstanceDir(), dir)
}

var _ backend.Placement = (*fakePlacement)(nil)

// TestPlanInstanceMatchesGoldenFiles plans each golden case at a fake placement
// and compares the rendered configs and snippets with the golden files the
// service's own writes produced, after the same placeholder substitution. It
// checks the file modes and the generated TLS material too.
func TestPlanInstanceMatchesGoldenFiles(t *testing.T) {
	for _, tc := range goldenCases() {
		t.Run(tc.name, func(t *testing.T) {
			id := nuid.Next()
			place := newFakePlacement(id)

			plan, err := planInstance(tc.spec, id, place, server.VERSION)
			if err != nil {
				t.Fatalf("planInstance: %v", err)
			}

			rendered := map[string][]byte{}
			for _, n := range plan.Nodes {
				rendered[filepath.Join(n.Dir, "config.cfg")] = n.Config

				for _, f := range n.Files {
					if f.Mode != 0600 {
						t.Errorf("%s mode = %o, want 600", f.Path, f.Mode)
					}
					rendered[f.Path] = f.Data
				}
			}

			compareGolden(t, tc.name, normalizeRendered(rendered, place.InstanceDir(), id))

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
// addresses in the order the create handlers have always reserved ports.
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
				[]string{"c1-n1/gateway", "c1-n2/gateway", "c2-n1/gateway", "c2-n2/gateway"},
				[]string{"c1-n1/cluster", "c1-n2/cluster"}, node("c1-n1"), node("c1-n2"),
				[]string{"c2-n1/cluster", "c2-n2/cluster"}, node("c2-n1"), node("c2-n2"),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.spec.Snippets = snippets
			tt.spec.MainTemplate = serverConfigTemplate

			id := nuid.Next()
			place := newFakePlacement(id)

			_, err := planInstance(tt.spec, id, place, server.VERSION)
			if err != nil {
				t.Fatalf("planInstance: %v", err)
			}

			if !slices.Equal(place.calls, tt.want) {
				t.Errorf("listener addresses asked for in order\n%v\nwant\n%v", place.calls, tt.want)
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
	place := newFakePlacement(id)
	spec := instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2, MainTemplate: serverConfigTemplate, Proxy: true}

	plan, err := planInstance(spec, id, place, server.VERSION)
	if err != nil {
		t.Fatalf("planInstance: %v", err)
	}

	wantClusters := []string{"c1", "c2"}
	if !slices.Equal(plan.Clusters, wantClusters) {
		t.Errorf("clusters = %v, want %v", plan.Clusters, wantClusters)
	}

	var names []string
	for _, n := range plan.nodes {
		names = append(names, n.Name)
		wantCluster := wantClusters[n.ClusterIndex-1]
		if n.Cluster != wantCluster {
			t.Errorf("node %s cluster = %q, want %q", n.Name, n.Cluster, wantCluster)
		}
		if n.TemplateData == nil || n.TemplateData.ServerName != n.Name {
			t.Errorf("node %s has no template data of its own", n.Name)
		}
	}
	wantNames := []string{"c1-n1", "c1-n2", "c2-n1", "c2-n2"}
	if !slices.Equal(names, wantNames) {
		t.Errorf("nodes = %v, want %v", names, wantNames)
	}

	byName := map[string]*nodePlan{}
	for _, n := range plan.nodes {
		byName[n.Name] = n
	}

	routes := map[string]int{}
	gateways := map[string]int{}
	for _, lk := range plan.Links {
		if lk.Node == lk.Peer {
			t.Errorf("node %s links to itself", lk.Node)
		}

		peer := byName[lk.Peer]
		var want backend.Listener
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

		if lk.Dial != want.Endpoint {
			t.Errorf("%s link from %s to %s dials %s, want %s", lk.Kind, lk.Node, lk.Peer, lk.Dial, want.Endpoint)
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
	plan, err = planInstance(spec, id, newFakePlacement(id), server.VERSION)
	if err != nil {
		t.Fatalf("planInstance without the proxy: %v", err)
	}
	if plan.ProxyNode != "" {
		t.Errorf("proxy node = %q without the proxy, want none", plan.ProxyNode)
	}
}

// TestPlanInstanceCaptureProxies proves a traced or proxied instance asks for a
// trace port per node, in start order and after every other address, so its
// listeners and link proxies get the same ports as the same instance untraced,
// with and without toxics, and that an untraced instance asks for none.
func TestPlanInstanceCaptureProxies(t *testing.T) {
	for _, toxics := range []bool{false, true} {
		t.Run(fmt.Sprintf("toxics=%t", toxics), func(t *testing.T) {
			id := nuid.Next()
			planWith := func(proxy bool) (*instancePlan, *fakePlacement) {
				place := newFakePlacement(id)
				spec := instanceSpec{Kind: "cluster", Servers: 3, MainTemplate: serverConfigTemplate, Proxy: proxy, Toxics: toxics}
				plan, err := planInstance(spec, id, place, server.VERSION)
				if err != nil {
					t.Fatalf("planInstance: %v", err)
				}
				return plan, place
			}

			plain, plainPlace := planWith(false)
			traced, tracedPlace := planWith(true)

			if plain.captureProxies != nil {
				t.Errorf("untraced plan has capture proxies %v", plain.captureProxies)
			}
			for _, call := range plainPlace.proxyCalls {
				if strings.HasPrefix(call, "trace ") {
					t.Errorf("untraced plan asked for %q", call)
				}
			}

			if !slices.Equal(tracedPlace.calls, plainPlace.calls) {
				t.Errorf("listener calls = %v traced, want the untraced %v", tracedPlace.calls, plainPlace.calls)
			}
			for i, n := range traced.nodes {
				if !reflect.DeepEqual(n.Listeners, plain.nodes[i].Listeners) {
					t.Errorf("node %s listeners %v traced, want the untraced %v", n.Name, n.Listeners, plain.nodes[i].Listeners)
				}
			}
			if !reflect.DeepEqual(traced.clientProxies, plain.clientProxies) {
				t.Errorf("client proxies %v traced, want the untraced %v", traced.clientProxies, plain.clientProxies)
			}

			want := slices.Clone(plainPlace.proxyCalls)
			for _, n := range traced.nodes {
				want = append(want, "trace trace:"+n.Name)
			}
			if !slices.Equal(tracedPlace.proxyCalls, want) {
				t.Errorf("proxy calls = %v, want %v", tracedPlace.proxyCalls, want)
			}

			if len(traced.captureProxies) != len(traced.nodes) {
				t.Fatalf("traced plan has %d capture proxies, want %d", len(traced.captureProxies), len(traced.nodes))
			}
			seen := map[int]bool{}
			for _, n := range traced.nodes {
				ep, ok := traced.captureProxies[n.Name]
				if !ok {
					t.Fatalf("node %s has no capture proxy", n.Name)
				}
				if seen[ep.Port] {
					t.Errorf("node %s shares capture proxy port %d", n.Name, ep.Port)
				}
				seen[ep.Port] = true
			}
		})
	}
}

// TestPlanInstanceDirVersionPersistent proves the planner names each node's
// directory as the in-process layout does and prefixes every one of its files
// with it, gives every node the version it is asked for, and records the store
// directory placement returns as the node's persistent path and its rendered
// .StoreDir. The exported nodes are the wrappers' embedded plans.
func TestPlanInstanceDirVersionPersistent(t *testing.T) {
	tests := []struct {
		name string
		spec instanceSpec
		dirs []string
	}{
		{
			name: "server",
			spec: instanceSpec{Kind: "server", Servers: 1},
			dirs: []string{"n1"},
		},
		{
			name: "cluster",
			spec: instanceSpec{Kind: "cluster", Servers: 3},
			dirs: []string{"n1", "n2", "n3"},
		},
		{
			name: "super-cluster",
			spec: instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2},
			dirs: []string{"c1_s1", "c1_s2", "c2_s1", "c2_s2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.spec.MainTemplate = serverConfigTemplate
			tt.spec.Snippets = map[string]string{"top": "", "websocket": ""}
			tt.spec.TLS = true

			id := nuid.Next()
			place := newFakePlacement(id)

			plan, err := planInstance(tt.spec, id, place, "9.8.7")
			if err != nil {
				t.Fatalf("planInstance: %v", err)
			}

			if len(plan.Nodes) != len(tt.dirs) || len(plan.nodes) != len(tt.dirs) {
				t.Fatalf("plan has %d exported and %d wrapped nodes, want %d", len(plan.Nodes), len(plan.nodes), len(tt.dirs))
			}

			for i, n := range plan.nodes {
				if plan.Nodes[i] != &n.NodePlan {
					t.Errorf("exported node %d is not node %s's embedded plan", i, n.Name)
				}
				if n.Dir != tt.dirs[i] {
					t.Errorf("node %s dir = %q, want %q", n.Name, n.Dir, tt.dirs[i])
				}
				if len(n.Files) == 0 {
					t.Errorf("node %s has no files", n.Name)
				}
				for _, f := range n.Files {
					if !strings.HasPrefix(f.Path, n.Dir+string(filepath.Separator)) {
						t.Errorf("node %s file %s is not under its dir %s", n.Name, f.Path, n.Dir)
					}
				}
				if n.Version != "9.8.7" {
					t.Errorf("node %s version = %q, want 9.8.7", n.Name, n.Version)
				}

				store := place.StoreDir(n.Dir)
				if !slices.Equal(n.Persistent, []string{store}) {
					t.Errorf("node %s persistent = %v, want [%s]", n.Name, n.Persistent, store)
				}
				if n.TemplateData.StoreDir != store {
					t.Errorf("node %s renders store dir %q, want %q", n.Name, n.TemplateData.StoreDir, store)
				}
			}
		})
	}
}

// storePlacement is a fakePlacement whose store directories lie under a store
// directory of their own, apart from the node directories, so a store
// directory worked out from anything but the node's Dir shows. It records the
// argument of every StoreDir call.
type storePlacement struct {
	*fakePlacement
	asked []string
}

func (p *storePlacement) StoreDir(dir string) string {
	p.asked = append(p.asked, dir)
	return filepath.Join(p.InstanceDir(), "store", dir)
}

// TestPlanInstanceStoreDirFollowsDir proves the planner asks placement for each
// node's store directory by the node's Dir, never by its Name, and records the
// answer as the node's persistent path and its rendered .StoreDir. The server
// and cluster are named, since their default names, n<i>, are their dirs.
func TestPlanInstanceStoreDirFollowsDir(t *testing.T) {
	specs := []instanceSpec{
		{Kind: "server", Servers: 1, ServerName: "solo"},
		{Kind: "cluster", ClusterList: []clusterSpec{{Name: "east", Servers: 3}}},
		{Kind: "super-cluster", Servers: 2, Clusters: 2},
	}

	for _, spec := range specs {
		t.Run(spec.Kind, func(t *testing.T) {
			spec.MainTemplate = serverConfigTemplate

			id := nuid.Next()
			place := &storePlacement{fakePlacement: newFakePlacement(id)}

			plan, err := planInstance(spec, id, place, server.VERSION)
			if err != nil {
				t.Fatalf("planInstance: %v", err)
			}

			var dirs []string
			for _, n := range plan.nodes {
				dirs = append(dirs, n.Dir)
				if n.Name == n.Dir {
					t.Fatalf("node %s is named for its dir, so the test cannot tell them apart", n.Name)
				}

				want := filepath.Join(place.InstanceDir(), "store", n.Dir)
				if !slices.Equal(n.Persistent, []string{want}) {
					t.Errorf("node %s persistent = %v, want [%s]", n.Name, n.Persistent, want)
				}
				if n.TemplateData.StoreDir != want {
					t.Errorf("node %s renders store dir %q, want %q", n.Name, n.TemplateData.StoreDir, want)
				}
			}

			if !slices.Equal(place.asked, dirs) {
				t.Errorf("StoreDir asked for %v, want the node dirs %v", place.asked, dirs)
			}
		})
	}
}

// TestPlanInstanceErrors proves each kind of planning failure is returned with
// the message the create handlers answer it with.
func TestPlanInstanceErrors(t *testing.T) {
	tests := []struct {
		name string
		spec instanceSpec
		// failAt is the ListenerAddress call that fails, 0 for none.
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
			// A cluster of 3 has 6 listeners, so the 7th call asks for the
			// first route proxy.
			name:   "proxy port",
			spec:   instanceSpec{Kind: "cluster", Servers: 3, Toxics: true},
			failAt: 7,
			kind:   planErrPort,
			prefix: "could not get free port: proxy route:",
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
			place.failAt = tt.failAt

			plan, err := planInstance(tt.spec, id, place, server.VERSION)
			if err == nil {
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
		})
	}
}

// TestPlanInstanceClusterList proves a super-cluster planned from a cluster
// list of lon with five servers and nyc with three has eight nodes in two
// clusters of their own sizes and names: routes only within each cluster,
// gateways named lon and nyc, a .ClusterSize of each node's own cluster, and
// the names in server_name, the link names and the nodes the agent is handed.
func TestPlanInstanceClusterList(t *testing.T) {
	id := nuid.Next()
	spec := instanceSpec{
		Kind:         "super-cluster",
		ClusterList:  []clusterSpec{{Name: "lon", Servers: 5}, {Name: "nyc", Servers: 3}},
		MainTemplate: serverConfigTemplate,
		Toxics:       true,
	}

	plan, err := planInstance(spec, id, newFakePlacement(id), server.VERSION)
	if err != nil {
		t.Fatalf("planInstance: %v", err)
	}

	if !slices.Equal(plan.Clusters, []string{"lon", "nyc"}) {
		t.Errorf("clusters = %v, want [lon nyc]", plan.Clusters)
	}

	wantNames := []string{"lon-n1", "lon-n2", "lon-n3", "lon-n4", "lon-n5", "nyc-n1", "nyc-n2", "nyc-n3"}
	wantDirs := []string{"c1_s1", "c1_s2", "c1_s3", "c1_s4", "c1_s5", "c2_s1", "c2_s2", "c2_s3"}
	var names, dirs []string
	size := map[string]int{"lon": 5, "nyc": 3}
	for _, n := range plan.nodes {
		names = append(names, n.Name)
		dirs = append(dirs, n.Dir)

		td := n.TemplateData
		if td.ClusterSize != size[n.Cluster] {
			t.Errorf("node %s .ClusterSize = %d, want %d", n.Name, td.ClusterSize, size[n.Cluster])
		}
		if len(td.Routes) != size[n.Cluster] {
			t.Errorf("node %s has %d routes, want %d", n.Name, len(td.Routes), size[n.Cluster])
		}
		if len(td.Gateways["lon"]) != 5 || len(td.Gateways["nyc"]) != 3 || len(td.Gateways) != 2 {
			t.Errorf("node %s gateways = %v, want lon with 5 and nyc with 3", n.Name, td.Gateways)
		}

		config := string(n.Config)
		for _, want := range []string{
			fmt.Sprintf("server_name: %q", n.Name),
			fmt.Sprintf("name: %q", n.Cluster),
			`{ name: "lon", urls: [`,
			`{ name: "nyc", urls: [`,
		} {
			if !strings.Contains(config, want) {
				t.Errorf("node %s config does not hold %s", n.Name, want)
			}
		}
	}
	if !slices.Equal(names, wantNames) {
		t.Errorf("nodes = %v, want %v", names, wantNames)
	}
	if !slices.Equal(dirs, wantDirs) {
		t.Errorf("node dirs = %v, want %v", dirs, wantDirs)
	}

	cluster := map[string]string{}
	for _, n := range plan.nodes {
		cluster[n.Name] = n.Cluster
	}

	routes := 0
	for _, lk := range plan.Links {
		if lk.Kind == "route" {
			routes++
			if cluster[lk.Node] != cluster[lk.Peer] {
				t.Errorf("route link from %s to %s crosses clusters", lk.Node, lk.Peer)
			}
		}
	}
	if routes != 5*4+3*2 {
		t.Errorf("plan has %d route links, want %d", routes, 5*4+3*2)
	}

	var proxies []string
	for _, p := range plan.proxies {
		proxies = append(proxies, p.Name)
	}
	for _, want := range []string{"route:lon-n1->lon-n5", "route:nyc-n3->nyc-n1", "gateway:lon->nyc-n2", "gateway:nyc->lon-n4"} {
		if !slices.Contains(proxies, want) {
			t.Errorf("proxies %v do not hold %s", proxies, want)
		}
	}
	// 26 routes, then a gateway proxy from lon to each of nyc's 3 servers
	// and from nyc to each of lon's 5.
	if len(proxies) != 26+3+5 {
		t.Errorf("plan has %d route and gateway proxies, want %d", len(proxies), 26+3+5)
	}
	for _, name := range wantNames {
		_, ok := plan.clientProxies[name]
		if !ok {
			t.Errorf("node %s has no client proxy", name)
		}
	}
}

// TestPlanInstanceGivenServerNames proves the names a create gives the
// servers become their plan names, and with them each server_name, route link
// name and .ServerName, while the directories follow the positions.
func TestPlanInstanceGivenServerNames(t *testing.T) {
	id := nuid.Next()
	spec := instanceSpec{
		Kind:         "cluster",
		ClusterList:  []clusterSpec{{Name: "east", Servers: 2, ServerNames: []string{"alpha", "beta"}}},
		MainTemplate: serverConfigTemplate,
		Toxics:       true,
	}

	plan, err := planInstance(spec, id, newFakePlacement(id), server.VERSION)
	if err != nil {
		t.Fatalf("planInstance: %v", err)
	}

	if !slices.Equal(plan.Clusters, []string{"east"}) {
		t.Errorf("clusters = %v, want [east]", plan.Clusters)
	}

	for i, want := range []string{"alpha", "beta"} {
		n := plan.nodes[i]
		if n.Name != want || n.Cluster != "east" || n.Dir != fmt.Sprintf("n%d", i+1) {
			t.Errorf("node %d = %s in %s at %s, want %s in east at n%d", i+1, n.Name, n.Cluster, n.Dir, want, i+1)
		}
		if n.TemplateData.ServerName != want {
			t.Errorf("node %s .ServerName = %q", want, n.TemplateData.ServerName)
		}
		if !strings.Contains(string(n.Config), fmt.Sprintf("server_name: %q", want)) {
			t.Errorf("node %s config does not set its server_name", want)
		}
	}

	var proxies []string
	for _, p := range plan.proxies {
		proxies = append(proxies, p.Name)
	}
	if !slices.Equal(proxies, []string{"route:alpha->beta", "route:beta->alpha"}) {
		t.Errorf("proxies = %v, want the routes between alpha and beta", proxies)
	}
}

// TestPlanNames proves the names the planner gives an instance: an unnamed
// instance's names carry no id, a server of a named cluster not named itself
// gets <cluster>-n<i>, an unnamed cluster of a cluster list keeps the default
// name of its position, c<c>, and a set holding one name twice, given or
// default, is refused naming who holds it.
func TestPlanNames(t *testing.T) {
	tests := []struct {
		name    string
		spec    instanceSpec
		want    []namedCluster
		wantErr string
	}{
		{
			name: "server",
			spec: instanceSpec{Kind: "server", Servers: 1},
			want: []namedCluster{{Servers: []string{"n1"}}},
		},
		{
			name: "named server",
			spec: instanceSpec{Kind: "server", Servers: 1, ServerName: "solo"},
			want: []namedCluster{{Servers: []string{"solo"}}},
		},
		{
			name: "cluster",
			spec: instanceSpec{Kind: "cluster", Servers: 2},
			want: []namedCluster{{Name: "c1", Servers: []string{"n1", "n2"}}},
		},
		{
			name: "named cluster",
			spec: instanceSpec{Kind: "cluster", ClusterList: []clusterSpec{{Name: "east", Servers: 2}}},
			want: []namedCluster{{Name: "east", Servers: []string{"east-n1", "east-n2"}}},
		},
		{
			name: "named servers of an unnamed cluster",
			spec: instanceSpec{Kind: "cluster", ClusterList: []clusterSpec{{Servers: 2, ServerNames: []string{"a", "b"}}}},
			want: []namedCluster{{Name: "c1", Servers: []string{"a", "b"}}},
		},
		{
			name: "super-cluster",
			spec: instanceSpec{Kind: "super-cluster", Servers: 2, Clusters: 2},
			want: []namedCluster{
				{Name: "c1", Servers: []string{"c1-n1", "c1-n2"}},
				{Name: "c2", Servers: []string{"c2-n1", "c2-n2"}},
			},
		},
		{
			name: "cluster list with an unnamed cluster",
			spec: instanceSpec{Kind: "super-cluster", ClusterList: []clusterSpec{{Name: "lon", Servers: 2}, {Servers: 1}, {Servers: 2, ServerNames: []string{"x", "y"}}}},
			want: []namedCluster{
				{Name: "lon", Servers: []string{"lon-n1", "lon-n2"}},
				{Name: "c2", Servers: []string{"c2-n1"}},
				{Name: "c3", Servers: []string{"x", "y"}},
			},
		},
		{
			name:    "duplicate cluster name",
			spec:    instanceSpec{Kind: "super-cluster", ClusterList: []clusterSpec{{Name: "lon", Servers: 2}, {Name: "lon", Servers: 2}}},
			wantErr: `names must be unique, "lon" names both cluster lon and cluster lon`,
		},
		{
			name:    "duplicate server name",
			spec:    instanceSpec{Kind: "super-cluster", ClusterList: []clusterSpec{{Name: "lon", Servers: 1, ServerNames: []string{"a"}}, {Name: "nyc", Servers: 2, ServerNames: []string{"b", "a"}}}},
			wantErr: `names must be unique, "a" names both server 1 of cluster lon and server 2 of cluster nyc`,
		},
		{
			name:    "server named like a cluster",
			spec:    instanceSpec{Kind: "super-cluster", ClusterList: []clusterSpec{{Name: "lon", Servers: 1}, {Name: "nyc", Servers: 2, ServerNames: []string{"lon", "b"}}}},
			wantErr: `names must be unique, "lon" names both cluster lon and server 1 of cluster nyc`,
		},
		{
			name:    "given name clashing with a derived one",
			spec:    instanceSpec{Kind: "super-cluster", ClusterList: []clusterSpec{{Name: "lon", Servers: 2}, {Name: "nyc", Servers: 1, ServerNames: []string{"lon-n2"}}}},
			wantErr: `names must be unique, "lon-n2" names both server 2 of cluster lon and server 1 of cluster nyc`,
		},
		{
			name:    "given cluster name clashing with a default one",
			spec:    instanceSpec{Kind: "super-cluster", ClusterList: []clusterSpec{{Servers: 1}, {Name: "c1", Servers: 1}}},
			wantErr: `names must be unique, "c1" names both cluster c1 and cluster c1`,
		},
		{
			name:    "given server name clashing with a default cluster name",
			spec:    instanceSpec{Kind: "cluster", ClusterList: []clusterSpec{{Servers: 2, ServerNames: []string{"c1", "b"}}}},
			wantErr: `names must be unique, "c1" names both cluster c1 and server 1 of cluster c1`,
		},
		{
			name:    "given server name clashing with a default server name",
			spec:    instanceSpec{Kind: "super-cluster", ClusterList: []clusterSpec{{Servers: 2}, {Name: "nyc", Servers: 1, ServerNames: []string{"c1-n2"}}}},
			wantErr: `names must be unique, "c1-n2" names both server 2 of cluster c1 and server 1 of cluster nyc`,
		},
		{
			name:    "server named like its own cluster",
			spec:    instanceSpec{Kind: "cluster", ClusterList: []clusterSpec{{Name: "east", Servers: 2, ServerNames: []string{"east", "b"}}}},
			wantErr: `names must be unique, "east" names both cluster east and server 1 of cluster east`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := planNames(tt.spec)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("planNames error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("planNames: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("planNames = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestPlanInstanceNameClash proves planning an instance whose names clash
// fails with a planErrNames error, answered with 002 and an Invalid request
// prefix.
func TestPlanInstanceNameClash(t *testing.T) {
	id := nuid.Next()
	spec := instanceSpec{Kind: "super-cluster", ClusterList: []clusterSpec{{Name: "lon", Servers: 2}, {Name: "nyc", Servers: 2, ServerNames: []string{"lon-n1", "b"}}}, MainTemplate: serverConfigTemplate}

	_, err := planInstance(spec, id, newFakePlacement(id), server.VERSION)

	var perr *planError
	if !errors.As(err, &perr) || perr.Kind != planErrNames {
		t.Fatalf("planInstance error = %v, want a planErrNames error", err)
	}
	if planErrorCode(err) != "002" {
		t.Errorf("code = %q, want 002", planErrorCode(err))
	}
	want := `Invalid request: names must be unique, "lon-n1" names both server 1 of cluster lon and server 1 of cluster nyc`
	if err.Error() != want {
		t.Errorf("error = %q, want %q", err, want)
	}
}
