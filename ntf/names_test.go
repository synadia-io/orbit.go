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
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// TestNameRule proves the rule every given name follows: a letter, then
// letters, digits, _ or -, at most 32 characters for a server and 28 for a
// cluster.
func TestNameRule(t *testing.T) {
	tests := []struct {
		name    string
		server  bool
		cluster bool
	}{
		{"lon", true, true},
		{"a", true, true},
		{"Lon_1-b", true, true},
		{strings.Repeat("a", 28), true, true},
		{strings.Repeat("a", 29), true, false},
		{strings.Repeat("a", 32), true, false},
		{strings.Repeat("a", 33), false, false},
		{"", false, false},
		{"1lon", false, false},
		{"-lon", false, false},
		{"_lon", false, false},
		{"lon.n1", false, false},
		{"lon n1", false, false},
		{"lon*", false, false},
		{"lon>", false, false},
		{"lön", false, false},
	}

	for _, tt := range tests {
		err := checkGivenServerName("name", tt.name)
		if (err == nil) != tt.server {
			t.Errorf("server name %q: error %v, want valid %t", tt.name, err, tt.server)
		}
		err = checkGivenClusterName("name", tt.name)
		if (err == nil) != tt.cluster {
			t.Errorf("cluster name %q: error %v, want valid %t", tt.name, err, tt.cluster)
		}
	}
}

// specRefused checks serr is a refusal with code and message want.
func specRefused(t *testing.T, serr *specError, code, want string) {
	t.Helper()

	if serr == nil {
		t.Fatal("request accepted, want a refusal")
	}
	if serr.Code != code {
		t.Errorf("code = %q, want %q", serr.Code, code)
	}
	if serr.Error() != want {
		t.Errorf("message = %q, want %q", serr.Error(), want)
	}
}

// TestClusterRequestClusters proves the cluster a cluster create asks for: its
// size from servers or server_names, which must agree when both are given,
// within today's bounds, and its given names checked against the name rule.
func TestClusterRequestClusters(t *testing.T) {
	long := strings.Repeat("a", 33)

	tests := []struct {
		name    string
		req     api.CreateClusterRequest
		want    []clusterSpec
		code    string
		wantErr string
	}{
		{
			name: "servers",
			req:  api.CreateClusterRequest{Servers: 3},
			want: []clusterSpec{{Servers: 3}},
		},
		{
			name: "names",
			req:  api.CreateClusterRequest{ClusterName: "east", ServerNames: []string{"a", "b"}},
			want: []clusterSpec{{Name: "east", Servers: 2, ServerNames: []string{"a", "b"}}},
		},
		{
			name: "servers and names agreeing",
			req:  api.CreateClusterRequest{Servers: 2, ServerNames: []string{"a", "b"}},
			want: []clusterSpec{{Servers: 2, ServerNames: []string{"a", "b"}}},
		},
		{
			name:    "servers and names disagreeing",
			req:     api.CreateClusterRequest{Servers: 3, ServerNames: []string{"a", "b"}},
			code:    "002",
			wantErr: "Invalid request: the cluster has servers 3 but 2 server_names",
		},
		{
			name:    "neither",
			req:     api.CreateClusterRequest{ClusterName: "east"},
			code:    "002",
			wantErr: "Invalid request: the cluster needs servers or server_names",
		},
		{
			name:    "one name",
			req:     api.CreateClusterRequest{ServerNames: []string{"a"}},
			code:    "002",
			wantErr: "Invalid request: servers should be at least 2",
		},
		{
			name:    "too many",
			req:     api.CreateClusterRequest{Servers: maxServersPerCluster + 1},
			code:    "002",
			wantErr: fmt.Sprintf("Invalid request: servers should be at most %d", maxServersPerCluster),
		},
		{
			name:    "cluster name starting with a digit",
			req:     api.CreateClusterRequest{Servers: 2, ClusterName: "1east"},
			code:    "002",
			wantErr: `Invalid request: cluster_name must start with a letter and hold only letters, digits, _ and -, at most 28 characters, not "1east"`,
		},
		{
			name:    "cluster name of 29 characters",
			req:     api.CreateClusterRequest{Servers: 2, ClusterName: strings.Repeat("e", 29)},
			code:    "002",
			wantErr: fmt.Sprintf(`Invalid request: cluster_name must start with a letter and hold only letters, digits, _ and -, at most 28 characters, not %q`, strings.Repeat("e", 29)),
		},
		{
			name:    "server name starting with -",
			req:     api.CreateClusterRequest{ClusterName: "east", ServerNames: []string{"a", "-b"}},
			code:    "002",
			wantErr: `Invalid request: the name of server 2 of cluster east must start with a letter and hold only letters, digits, _ and -, at most 32 characters, not "-b"`,
		},
		{
			name:    "server name of 33 characters",
			req:     api.CreateClusterRequest{ServerNames: []string{long, "b"}},
			code:    "002",
			wantErr: fmt.Sprintf(`Invalid request: the name of server 1 of the cluster must start with a letter and hold only letters, digits, _ and -, at most 32 characters, not %q`, long),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := clusterRequestClusters(tt.req)
			if tt.wantErr != "" {
				specRefused(t, err, tt.code, tt.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("clusterRequestClusters: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("clusters = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestSuperClusterRequestClusters proves the clusters a super-cluster create
// asks for: clusters and servers or cluster_list, never both and never
// neither, each listed cluster with its size from servers or server_names,
// which must agree, of one to twenty servers, and its given names checked
// against the name rule. Every refusal names the cluster it is about.
func TestSuperClusterRequestClusters(t *testing.T) {
	tests := []struct {
		name    string
		req     api.CreateSuperClusterRequest
		want    []clusterSpec
		code    string
		wantErr string
	}{
		{
			name: "counts",
			req:  api.CreateSuperClusterRequest{Clusters: 2, Servers: 3},
		},
		{
			name: "cluster list",
			req: api.CreateSuperClusterRequest{ClusterList: []api.ClusterSpec{
				{Name: "lon", Servers: 5},
				{Name: "nyc", ServerNames: []string{"n1", "n2", "n3"}},
				{Servers: 1},
				{Servers: 2, ServerNames: []string{"x", "y"}},
			}},
			want: []clusterSpec{
				{Name: "lon", Servers: 5},
				{Name: "nyc", Servers: 3, ServerNames: []string{"n1", "n2", "n3"}},
				{Servers: 1},
				{Servers: 2, ServerNames: []string{"x", "y"}},
			},
		},
		{
			name:    "both forms",
			req:     api.CreateSuperClusterRequest{Clusters: 2, Servers: 2, ClusterList: []api.ClusterSpec{{Servers: 2}, {Servers: 2}}},
			code:    "002",
			wantErr: "Invalid request: give clusters and servers or cluster_list, not both",
		},
		{
			name:    "servers with a cluster list",
			req:     api.CreateSuperClusterRequest{Servers: 2, ClusterList: []api.ClusterSpec{{Servers: 2}, {Servers: 2}}},
			code:    "002",
			wantErr: "Invalid request: give clusters and servers or cluster_list, not both",
		},
		{
			name:    "neither form",
			req:     api.CreateSuperClusterRequest{JetStream: true},
			code:    "002",
			wantErr: "Invalid request: give clusters and servers, or cluster_list",
		},
		{
			name:    "servers without clusters",
			req:     api.CreateSuperClusterRequest{Servers: 2},
			code:    "002",
			wantErr: "Invalid request: give clusters and servers together",
		},
		{
			name:    "clusters without servers",
			req:     api.CreateSuperClusterRequest{Clusters: 3},
			code:    "002",
			wantErr: "Invalid request: give clusters and servers together",
		},
		{
			name:    "one listed cluster",
			req:     api.CreateSuperClusterRequest{ClusterList: []api.ClusterSpec{{Servers: 2}}},
			code:    "004",
			wantErr: "Invalid request: cluster_list should hold at least 2 clusters",
		},
		{
			name:    "too many listed clusters",
			req:     api.CreateSuperClusterRequest{ClusterList: make([]api.ClusterSpec, maxClusters+1)},
			code:    "004",
			wantErr: fmt.Sprintf("Invalid request: cluster_list should hold at most %d clusters", maxClusters),
		},
		{
			name:    "listed cluster with neither",
			req:     api.CreateSuperClusterRequest{ClusterList: []api.ClusterSpec{{Servers: 2}, {}}},
			code:    "002",
			wantErr: "Invalid request: cluster 2 needs servers or server_names",
		},
		{
			name:    "size disagreeing with names",
			req:     api.CreateSuperClusterRequest{ClusterList: []api.ClusterSpec{{Servers: 2}, {Name: "nyc", Servers: 3, ServerNames: []string{"a", "b"}}}},
			code:    "002",
			wantErr: "Invalid request: cluster nyc has servers 3 but 2 server_names",
		},
		{
			name:    "listed cluster too big",
			req:     api.CreateSuperClusterRequest{ClusterList: []api.ClusterSpec{{Name: "lon", Servers: maxServersPerCluster + 1}, {Servers: 2}}},
			code:    "002",
			wantErr: fmt.Sprintf("Invalid request: cluster lon should have at most %d servers", maxServersPerCluster),
		},
		{
			name:    "cluster name starting with a digit",
			req:     api.CreateSuperClusterRequest{ClusterList: []api.ClusterSpec{{Name: "lon", Servers: 2}, {Name: "9nyc", Servers: 2}}},
			code:    "002",
			wantErr: `Invalid request: the name of cluster 2 must start with a letter and hold only letters, digits, _ and -, at most 28 characters, not "9nyc"`,
		},
		{
			name:    "cluster name of 29 characters",
			req:     api.CreateSuperClusterRequest{ClusterList: []api.ClusterSpec{{Name: strings.Repeat("l", 29), Servers: 2}, {Servers: 2}}},
			code:    "002",
			wantErr: fmt.Sprintf(`Invalid request: the name of cluster 1 must start with a letter and hold only letters, digits, _ and -, at most 28 characters, not %q`, strings.Repeat("l", 29)),
		},
		{
			name:    "server name of 33 characters",
			req:     api.CreateSuperClusterRequest{ClusterList: []api.ClusterSpec{{Servers: 2}, {Name: "nyc", ServerNames: []string{strings.Repeat("n", 33)}}}},
			code:    "002",
			wantErr: fmt.Sprintf(`Invalid request: the name of server 1 of cluster nyc must start with a letter and hold only letters, digits, _ and -, at most 32 characters, not %q`, strings.Repeat("n", 33)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := superClusterRequestClusters(tt.req)
			if tt.wantErr != "" {
				specRefused(t, err, tt.code, tt.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("superClusterRequestClusters: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("clusters = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestCreateNamedRefusals proves what the service answers a create whose
// names or sizes it refuses with: the schema refuses a name breaking the rule
// with 001, and the handlers refuse what the schema cannot say with 002.
func TestCreateNamedRefusals(t *testing.T) {
	svc := startTestService(t)

	long := strings.Repeat("a", 33)

	tests := []struct {
		name    string
		subject string
		body    string
		code    string
		want    string
	}{
		{"both forms", "tester.create.super-cluster", `{"clusters":2,"servers":2,"cluster_list":[{"servers":2},{"servers":2}]}`, "002", "Invalid request: give clusters and servers or cluster_list, not both"},
		{"neither form", "tester.create.super-cluster", `{}`, "002", "Invalid request: give clusters and servers, or cluster_list"},
		{"size disagreeing with names", "tester.create.super-cluster", `{"cluster_list":[{"servers":2},{"name":"nyc","servers":3,"server_names":["a","b"]}]}`, "002", "Invalid request: cluster nyc has servers 3 but 2 server_names"},
		{"cluster without servers", "tester.create.cluster", `{"cluster_name":"east"}`, "002", "Invalid request: the cluster needs servers or server_names"},
		{"cluster size disagreeing with names", "tester.create.cluster", `{"servers":3,"server_names":["a","b"]}`, "002", "Invalid request: the cluster has servers 3 but 2 server_names"},
		{"server name starting with a digit", "tester.create.server", `{"name":"1a"}`, "001", "Invalid request: name: '1a' does not match pattern '^[A-Za-z][A-Za-z0-9_-]*$'"},
		{"server name starting with -", "tester.create.cluster", `{"server_names":["a","-b"]}`, "001", "Invalid request: server_names.1: '-b' does not match pattern '^[A-Za-z][A-Za-z0-9_-]*$'"},
		{"server name of 33 characters", "tester.create.super-cluster", `{"cluster_list":[{"servers":2},{"server_names":["` + long + `"]}]}`, "001", "Invalid request: cluster_list.1.server_names.0: maxLength: got 33, want 32"},
		{"cluster name of 29 characters", "tester.create.cluster", `{"servers":2,"cluster_name":"` + strings.Repeat("e", 29) + `"}`, "001", "Invalid request: cluster_name: maxLength: got 29, want 28"},
		{"duplicate cluster name", "tester.create.super-cluster", `{"cluster_list":[{"name":"lon","servers":2},{"name":"lon","servers":2}]}`, "002", `Invalid request: names must be unique, "lon" names both cluster lon and cluster lon`},
		{"duplicate server name", "tester.create.cluster", `{"server_names":["a","a"]}`, "002", `Invalid request: names must be unique, "a" names both server 1 of cluster C_<SHORT> and server 2 of cluster C_<SHORT>`},
		{"server named like a cluster", "tester.create.super-cluster", `{"cluster_list":[{"name":"lon","servers":2},{"name":"nyc","server_names":["lon"]}]}`, "002", `Invalid request: names must be unique, "lon" names both cluster lon and server 1 of cluster nyc`},
		{"given name clashing with a derived one", "tester.create.super-cluster", `{"cluster_list":[{"name":"lon","servers":2},{"name":"nyc","server_names":["lon-n1"]}]}`, "002", `Invalid request: names must be unique, "lon-n1" names both server 1 of cluster lon and server 1 of cluster nyc`},
	}

	shortName := regexp.MustCompile(`C_[A-Za-z0-9]{8}`)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, desc := rawServiceError(t, svc.nc, tt.subject, []byte(tt.body))
			desc = shortName.ReplaceAllString(desc, "C_<SHORT>")
			if code != tt.code {
				t.Errorf("code = %q (%s), want %q", code, desc, tt.code)
			}
			if desc != tt.want {
				t.Errorf("description = %q, want %q", desc, tt.want)
			}
		})
	}

	var list api.ListResponse
	mustRequest(t, svc.nc, "tester.list", api.ListRequest{}, &list)
	if len(list.Instances) != 0 {
		t.Errorf("list holds %d instances after the refusals, want none", len(list.Instances))
	}
}

// TestCreateClusterListLeavesCountsOut proves a super-cluster create given
// through the Go type with a cluster list and zero counts sends no clusters or
// servers, and that the service creates it.
func TestCreateClusterListLeavesCountsOut(t *testing.T) {
	req := api.CreateSuperClusterRequest{ClusterList: []api.ClusterSpec{{Servers: 2}, {Name: "nyc", Servers: 1}}}

	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytes.Contains(payload, []byte(`"servers":0`)) || bytes.Contains(payload, []byte(`"clusters"`)) {
		t.Fatalf("request %s sends a zero count", payload)
	}

	svc := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.super-cluster", req, &created)
	if len(created.Servers) != 3 {
		t.Errorf("created %d servers, want 3", len(created.Servers))
	}
}

// TestCreateNamedSuperClusterWithToxics proves a super-cluster created with
// toxics from a cluster list of lon with three servers and nyc with two named
// a and b carries the names in its create response, its links, the topology
// its agent is handed and its status.
func TestCreateNamedSuperClusterWithToxics(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.super-cluster", api.CreateSuperClusterRequest{
		ClusterList: []api.ClusterSpec{{Name: "lon", Servers: 3}, {Name: "nyc", ServerNames: []string{"a", "b"}}},
		Toxics:      true,
	}, &created)

	wantNames := []string{"lon-n1", "lon-n2", "lon-n3", "a", "b"}
	wantClusters := []string{"lon", "lon", "lon", "nyc", "nyc"}

	var names, clusters []string
	for _, srv := range created.Servers {
		names = append(names, srv.Name)
		clusters = append(clusters, srv.Cluster)
	}
	if !slices.Equal(names, wantNames) || !slices.Equal(clusters, wantClusters) {
		t.Errorf("created servers %v in clusters %v, want %v in %v", names, clusters, wantNames, wantClusters)
	}

	var links []string
	for _, lk := range created.Links {
		links = append(links, lk.Name)
	}
	for _, want := range []string{"route:lon-n1->lon-n3", "route:a->b", "gateway:lon->a", "gateway:nyc->lon-n2", "client:a"} {
		if !slices.Contains(links, want) {
			t.Errorf("links %v do not hold %s", links, want)
		}
	}
	// 6 routes in lon and 2 in nyc, a gateway from lon to each of nyc's 2
	// servers and from nyc to each of lon's 3, and 5 clients.
	if len(links) != 8+5+5 {
		t.Errorf("create response has %d links, want %d", len(links), 8+5+5)
	}

	specs, _ := provider.snapshot()
	if len(specs) != 1 {
		t.Fatalf("agent was handed %d specs, want one", len(specs))
	}
	var topology []string
	for _, n := range specs[0].Nodes {
		topology = append(topology, n.Cluster+"/"+n.Name)
	}
	wantTopology := []string{"lon/lon-n1", "lon/lon-n2", "lon/lon-n3", "nyc/a", "nyc/b"}
	if !slices.Equal(topology, wantTopology) {
		t.Errorf("agent topology = %v, want %v", topology, wantTopology)
	}

	var status api.StatusResponse
	mustRequest(t, svc.nc, "tester.status", api.StatusRequest{InstanceID: created.ID}, &status)
	if len(status.Instances) != 1 {
		t.Fatalf("status holds %d instances, want 1", len(status.Instances))
	}
	names, clusters = nil, nil
	for _, srv := range status.Instances[0].Servers {
		names = append(names, srv.Name)
		clusters = append(clusters, srv.Cluster)
	}
	if !slices.Equal(names, wantNames) || !slices.Equal(clusters, wantClusters) {
		t.Errorf("status servers %v in clusters %v, want %v in %v", names, clusters, wantNames, wantClusters)
	}
}

// TestCreateNamedServerAndCluster proves a server and a cluster created with
// names report them in the create response, status and list.
func TestCreateNamedServerAndCluster(t *testing.T) {
	svc := startTestService(t)

	var single api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Name: "solo"}, &single)
	if len(single.Servers) != 1 || single.Servers[0].Name != "solo" || !single.Servers[0].Running {
		t.Errorf("created server %+v, want solo running", single.Servers)
	}

	var cluster api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{ClusterName: "west", Servers: 2}, &cluster)

	var names []string
	for _, srv := range cluster.Servers {
		names = append(names, srv.Cluster+"/"+srv.Name)
	}
	if !slices.Equal(names, []string{"west/west-n1", "west/west-n2"}) {
		t.Errorf("created cluster servers %v, want west/west-n1 and west/west-n2", names)
	}

	var list api.ListResponse
	mustRequest(t, svc.nc, "tester.list", api.ListRequest{}, &list)
	for _, inst := range list.Instances {
		if inst.ID == cluster.ID && (inst.Cluster != "west" || inst.Servers != 2) {
			t.Errorf("list reports the cluster as %q with %d servers, want west with 2", inst.Cluster, inst.Servers)
		}
	}
}

// TestCreateSuperClusterOfOneServerClusters proves a super-cluster may hold
// clusters of one server: a super-cluster of two one-server clusters with
// JetStream elects its meta leader across the gateway, places a stream in the
// other cluster and takes a publish into it.
func TestCreateSuperClusterOfOneServerClusters(t *testing.T) {
	svc := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.super-cluster", api.CreateSuperClusterRequest{
		ClusterList: []api.ClusterSpec{{Name: "a", Servers: 1}, {Name: "b", Servers: 1}},
		JetStream:   true,
	}, &created)

	if len(created.Servers) != 2 {
		t.Fatalf("created %d servers, want 2", len(created.Servers))
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://localhost:%d", created.Servers[0].Port))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "ONE", Subjects: []string{"one"}, Placement: &jetstream.Placement{Cluster: "b"}})
		if err == nil {
			_, err = js.Publish(ctx, "one", []byte("x"))
		}
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream in cluster b from cluster a: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
