package ntf

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/ntf/api"
)

// fakeService answers tester.* requests on a server of an instance of its own,
// so a Manager connected there talks to it alone and never to the real
// service. respond decides each answer from the subject and the decoded
// request: an empty code answers reply, any other code refuses the request
// with code and desc. Every request is recorded.
type fakeService struct {
	mu       sync.Mutex
	subjects []string
	requests []map[string]any
}

// startFakeService creates a server on the test service, answers on it with
// respond, and returns the fake and a Manager connected to it. Both go when the
// test ends.
func startFakeService(t *testing.T, respond func(subject string, req map[string]any) (code string, desc string, reply any)) (*fakeService, *Manager) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	real, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(real.Close)

	inst, err := real.CreateServer(ctx, false, WithDescription(t.Name()))
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	t.Cleanup(func() { inst.Destroy(t) })

	nc, err := nats.Connect(inst.Servers[0].URL)
	if err != nil {
		t.Fatalf("connect fake: %v", err)
	}
	t.Cleanup(nc.Close)

	f := &fakeService{}
	_, err = nc.Subscribe("tester.>", func(msg *nats.Msg) {
		req := map[string]any{}
		if len(msg.Data) > 0 {
			derr := json.Unmarshal(msg.Data, &req)
			if derr != nil {
				t.Errorf("fake: decode %s: %v", msg.Subject, derr)
			}
		}

		f.mu.Lock()
		f.subjects = append(f.subjects, msg.Subject)
		f.requests = append(f.requests, req)
		f.mu.Unlock()

		code, desc, reply := respond(msg.Subject, req)
		resp := nats.NewMsg(msg.Reply)
		if code != "" {
			resp.Header.Set("Nats-Service-Error", desc)
			resp.Header.Set("Nats-Service-Error-Code", code)
		} else {
			data, merr := json.Marshal(reply)
			if merr != nil {
				t.Errorf("fake: encode reply: %v", merr)
			}
			resp.Data = data
		}
		rerr := msg.RespondMsg(resp)
		if rerr != nil {
			t.Errorf("fake: respond: %v", rerr)
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	err = nc.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}

	m, err := Connect(ctx, inst.Servers[0].URL)
	if err != nil {
		t.Fatalf("connect manager to fake: %v", err)
	}
	t.Cleanup(m.Close)

	return f, m
}

// seen returns the requests the fake has received, in order.
func (f *fakeService) seen() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]map[string]any(nil), f.requests...)
}

// serverOpReply answers every stop, start, update and reload as done.
var serverOpReply = map[string]any{"shutdown": true, "started": true, "updated": true, "reloaded": true}

// TestCreateOptionsRefused pins that a naming option given to a create of the
// wrong kind, WithServerNames and WithClusters given empty, and WithClusters
// given with counts return ErrInvalidCreateOption before any request is sent.
func TestCreateOptionsRefused(t *testing.T) {
	f, m := startFakeService(t, func(string, map[string]any) (string, string, any) {
		return "", "", api.CreateResponse{}
	})
	ctx := context.Background()
	clusters := WithClusters(SizedCluster("a", 2), SizedCluster("b", 2))

	cases := []struct {
		name   string
		create func() (*Instance, error)
	}{
		{"server WithClusterName", func() (*Instance, error) { return m.CreateServer(ctx, false, WithClusterName("c")) }},
		{"server WithServerNames", func() (*Instance, error) { return m.CreateServer(ctx, false, WithServerNames("a", "b")) }},
		{"server WithClusters", func() (*Instance, error) { return m.CreateServer(ctx, false, clusters) }},
		{"cluster WithName", func() (*Instance, error) { return m.CreateCluster(ctx, 2, false, WithName("a")) }},
		{"cluster WithClusters", func() (*Instance, error) { return m.CreateCluster(ctx, 2, false, clusters) }},
		{"cluster WithServerNames without names", func() (*Instance, error) { return m.CreateCluster(ctx, 2, false, WithServerNames()) }},
		{"super-cluster WithName", func() (*Instance, error) { return m.CreateSuperCluster(ctx, 2, 2, false, WithName("a")) }},
		{"super-cluster WithClusterName", func() (*Instance, error) { return m.CreateSuperCluster(ctx, 2, 2, false, WithClusterName("c")) }},
		{"super-cluster WithServerNames", func() (*Instance, error) {
			return m.CreateSuperCluster(ctx, 2, 2, false, WithServerNames("a", "b"))
		}},
		{"WithClusters without clusters", func() (*Instance, error) { return m.CreateSuperCluster(ctx, 0, 0, false, WithClusters()) }},
		{"WithClusters and clusters", func() (*Instance, error) { return m.CreateSuperCluster(ctx, 2, 0, false, clusters) }},
		{"WithClusters and servers", func() (*Instance, error) { return m.CreateSuperCluster(ctx, 0, 3, false, clusters) }},
		{"WithClusters and both counts", func() (*Instance, error) { return m.CreateSuperCluster(ctx, 2, 3, false, clusters) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.create()
			if !errors.Is(err, ErrInvalidCreateOption) {
				t.Fatalf("err = %v, want ErrInvalidCreateOption", err)
			}
		})
	}

	if got := f.seen(); len(got) != 0 {
		t.Fatalf("the fake received %d requests, want none: %v", len(got), got)
	}
}

// TestCreateRequestsCarryNames pins what each naming option puts on the wire:
// the names, and with WithClusters the cluster list alone, without the counts
// the schema refuses next to it. The options copy what they are given, so the
// caller changing its slices after making them changes nothing.
func TestCreateRequestsCarryNames(t *testing.T) {
	f, m := startFakeService(t, func(string, map[string]any) (string, string, any) {
		return "", "", api.CreateResponse{ID: "fake"}
	})
	ctx := context.Background()

	_, err := m.CreateServer(ctx, false, WithName("alpha"))
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	names := []string{"e1", "e2"}
	serverNames := WithServerNames(names...)
	names[0] = "changed"
	_, err = m.CreateCluster(ctx, 0, false, WithClusterName("east"), serverNames)
	if err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	westNames := []string{"w1", "w2"}
	entries := []api.ClusterSpec{SizedCluster("east", 1), {Name: "west", ServerNames: westNames}, SizedCluster("", 2)}
	clusterList := WithClusters(entries...)
	entries[0].Name = "changed"
	westNames[0] = "changed"
	_, err = m.CreateSuperCluster(ctx, 0, 0, false, clusterList)
	if err != nil {
		t.Fatalf("create super-cluster with clusters: %v", err)
	}
	_, err = m.CreateSuperCluster(ctx, 2, 3, false)
	if err != nil {
		t.Fatalf("create super-cluster with counts: %v", err)
	}

	got := f.seen()
	if len(got) != 4 {
		t.Fatalf("the fake received %d requests, want 4", len(got))
	}

	if got[0]["name"] != "alpha" {
		t.Errorf("server request = %v, want name alpha", got[0])
	}

	cluster := got[1]
	if cluster["cluster_name"] != "east" {
		t.Errorf("cluster request = %v, want cluster_name east", cluster)
	}
	if names, _ := json.Marshal(cluster["server_names"]); string(names) != `["e1","e2"]` {
		t.Errorf("cluster server_names = %s, want [e1 e2]", names)
	}
	if _, ok := cluster["servers"]; ok {
		t.Errorf("cluster request = %v, want no servers count", cluster)
	}

	listed := got[2]
	for _, key := range []string{"clusters", "servers"} {
		if _, ok := listed[key]; ok {
			t.Errorf("super-cluster request with WithClusters = %v, want no %s", listed, key)
		}
	}
	list, _ := json.Marshal(listed["cluster_list"])
	want := `[{"name":"east","servers":1},{"name":"west","server_names":["w1","w2"]},{"servers":2}]`
	if string(list) != want {
		t.Errorf("cluster_list = %s, want %s", list, want)
	}

	counted := got[3]
	if counted["clusters"] != float64(2) || counted["servers"] != float64(3) {
		t.Errorf("super-cluster request with counts = %v, want clusters 2 and servers 3", counted)
	}
	if _, ok := counted["cluster_list"]; ok {
		t.Errorf("super-cluster request with counts = %v, want no cluster_list", counted)
	}
}

// TestServerCountFromOptions pins the server count the test helpers expect of a
// create: from the counts, or from the names and cluster list when the counts
// are left zero.
func TestServerCountFromOptions(t *testing.T) {
	cases := []struct {
		name     string
		opts     []CreateOption
		clusters int
		servers  int
		cluster  int
		super    int
	}{
		{name: "counts", clusters: 2, servers: 3, cluster: 3, super: 6},
		{name: "server names", opts: []CreateOption{WithServerNames("a", "b", "c")}, cluster: 3},
		{name: "server names and count", opts: []CreateOption{WithServerNames("a", "b")}, servers: 2, cluster: 2},
		{name: "cluster name only", opts: []CreateOption{WithClusterName("c")}, servers: 4, cluster: 4},
		{name: "cluster list", opts: []CreateOption{WithClusters(SizedCluster("a", 1), ClusterWithServerNames("b", "x", "y"), api.ClusterSpec{Servers: 3, ServerNames: []string{"p", "q", "r"}})}, super: 6},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			co := resolveCreateOptions(tc.opts)
			if got := co.clusterSize(tc.servers); got != tc.cluster {
				t.Errorf("cluster size = %d, want %d", got, tc.cluster)
			}
			if got := co.superClusterSize(tc.clusters, tc.servers); tc.super != 0 && got != tc.super {
				t.Errorf("super-cluster size = %d, want %d", got, tc.super)
			}
		})
	}
}

// oldSchemaRefusal is what services with request schemas from before
// instance_id (ntf #108 to #118) answer a stop, start, update or reload
// carrying it with, under code 001.
const oldSchemaRefusal = "Invalid request: additional properties 'instance_id' not allowed"

// currentService answers like a service that accepts instance_id: done for
// inst-1, 404 for any other instance, and 023 for the bare name, which two
// instances hold.
func currentService(_ string, req map[string]any) (string, string, any) {
	switch req["instance_id"] {
	case nil:
		return "023", "2 servers in instances inst-1, inst-2 are named \"n1\", send instance_id to choose one", nil
	case "inst-1":
		return "", "", serverOpReply
	default:
		return "404", "Instance not found", nil
	}
}

// oldSchemaService answers like a service with request schemas from before
// instance_id: it refuses the field, and does bare as bareCode says, "" for
// done.
func oldSchemaService(bareCode string) func(string, map[string]any) (string, string, any) {
	return func(_ string, req map[string]any) (string, string, any) {
		if _, ok := req["instance_id"]; ok {
			return "001", oldSchemaRefusal, nil
		}
		if bareCode != "" {
			return bareCode, "2 servers in instances inst-1, inst-2 are named \"n1\"", nil
		}
		return "", "", serverOpReply
	}
}

// TestInstanceServerRequests pins the per-instance server operations against a
// fake of each service generation: the instance ID sent first, a bare-name
// fallback only on the refusal of services with schemas from before
// instance_id, remembered for later calls, and the refusals returned as their
// sentinels. Every update request, the fallback too, carries the update
// options.
func TestInstanceServerRequests(t *testing.T) {
	const instanceID = "inst-1"

	ops := []struct {
		name     string
		subject  string
		instance func(m *Manager, id string) error
		bare     func(m *Manager) error
	}{
		{"stop", "tester.stop.server",
			func(m *Manager, id string) error {
				_, err := m.StopInstanceServer(context.Background(), id, "n1")
				return err
			},
			func(m *Manager) error { _, err := m.StopServer(context.Background(), "n1"); return err }},
		{"start", "tester.start.server",
			func(m *Manager, id string) error {
				_, err := m.StartInstanceServer(context.Background(), id, "n1")
				return err
			},
			func(m *Manager) error { _, err := m.StartServer(context.Background(), "n1"); return err }},
		{"update", "tester.update.server",
			func(m *Manager, id string) error {
				_, err := m.UpdateInstanceServer(context.Background(), id, "n1", updateOpts()...)
				return err
			},
			func(m *Manager) error {
				_, err := m.UpdateServer(context.Background(), "n1", updateOpts()...)
				return err
			}},
		{"reload", "tester.reload.server",
			func(m *Manager, id string) error {
				_, err := m.ReloadInstanceServer(context.Background(), id, "n1")
				return err
			},
			func(m *Manager) error { _, err := m.ReloadServer(context.Background(), "n1"); return err }},
	}

	scenarios := []struct {
		name    string
		respond func(string, map[string]any) (string, string, any)
		id      string
		// bare calls the method without an instance.
		bare bool
		// ids holds the instance_id of each request of the first call that
		// reaches the fake, "" for none, and laterIDs those of each later
		// call on the same Manager; nil means the same as ids.
		ids      []string
		laterIDs []string
		wantErr  []error
	}{
		{name: "current service gets the instance", respond: currentService, id: instanceID, ids: []string{instanceID}},
		{name: "unknown instance", respond: currentService, id: "inst-9", ids: []string{"inst-9"}, wantErr: []error{ErrService, ErrInstanceNotFound}},
		{name: "name not in the instance", id: instanceID, ids: []string{instanceID}, wantErr: []error{ErrService},
			respond: func(string, map[string]any) (string, string, any) { return "001", "Server not found", nil }},
		{name: "other 001 refusals are not a fallback", id: instanceID, ids: []string{instanceID}, wantErr: []error{ErrService},
			respond: func(string, map[string]any) (string, string, any) { return "001", "Invalid request", nil }},
		{name: "service before schemas ignores the instance", id: instanceID, ids: []string{instanceID},
			respond: func(string, map[string]any) (string, string, any) { return "", "", serverOpReply }},
		{name: "old schema falls back to the bare name and remembers", respond: oldSchemaService(""), id: instanceID,
			ids: []string{instanceID, ""}, laterIDs: []string{""}},
		{name: "old schema fallback refused as ambiguous", respond: oldSchemaService("023"), id: instanceID,
			ids: []string{instanceID, ""}, laterIDs: []string{""}, wantErr: []error{ErrService, ErrServerNameAmbiguous}},
		{name: "empty instance sends the bare name", respond: currentService, ids: []string{""}, wantErr: []error{ErrService, ErrServerNameAmbiguous}},
		{name: "bare method sends the bare name", respond: currentService, bare: true, ids: []string{""}, wantErr: []error{ErrService, ErrServerNameAmbiguous}},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			f, m := startFakeService(t, sc.respond)

			for call, op := range ops {
				before := len(f.seen())

				var err error
				if sc.bare {
					err = op.bare(m)
				} else {
					err = op.instance(m, sc.id)
				}

				for _, want := range sc.wantErr {
					if !errors.Is(err, want) {
						t.Errorf("%s: err = %v, want %v", op.name, err, want)
					}
				}
				if len(sc.wantErr) == 0 && err != nil {
					t.Errorf("%s: err = %v, want none", op.name, err)
				}

				ids := sc.ids
				if call > 0 && sc.laterIDs != nil {
					ids = sc.laterIDs
				}
				got := f.seen()[before:]
				if len(got) != len(ids) {
					t.Fatalf("%s: the fake received %d requests, want %d: %v", op.name, len(got), len(ids), got)
				}
				for i, req := range got {
					if req["name"] != "n1" {
						t.Errorf("%s: request %d = %v, want name n1", op.name, i, req)
					}
					id, _ := req["instance_id"].(string)
					if id != ids[i] {
						t.Errorf("%s: request %d instance_id = %q, want %q", op.name, i, id, ids[i])
					}
					if op.name == "update" {
						checkUpdateRequest(t, i, req)
					}
				}
				f.mu.Lock()
				subjects := f.subjects[before:]
				f.mu.Unlock()
				for _, s := range subjects {
					if s != op.subject {
						t.Errorf("%s: request on %s, want %s", op.name, s, op.subject)
					}
				}
			}
		})
	}
}

// updateOpts are the update options TestInstanceServerRequests sends, and
// checkUpdateRequest looks for.
func updateOpts() []UpdateOption {
	return []UpdateOption{WithTopLevel("debug: true"), WithTemplate("{{ .ServerName }}"), WithTLSTimeout(1500 * time.Millisecond)}
}

// checkUpdateRequest fails the test unless the update request req carries every
// option of updateOpts.
func checkUpdateRequest(t *testing.T, i int, req map[string]any) {
	t.Helper()

	snippets, _ := req["snippets"].(map[string]any)
	if snippets["top"] != "debug: true" {
		t.Errorf("update: request %d snippets = %v, want top", i, req["snippets"])
	}
	if req["template"] != "{{ .ServerName }}" {
		t.Errorf("update: request %d template = %v, want the template", i, req["template"])
	}
	if req["tls_timeout"] != 1.5 {
		t.Errorf("update: request %d tls_timeout = %v, want 1.5", i, req["tls_timeout"])
	}
}

// TestInstanceHelpersSendInstance pins that the Instance server helpers go
// through the per-instance operations, so each request carries the instance.
func TestInstanceHelpersSendInstance(t *testing.T) {
	f, m := startFakeService(t, currentService)

	inst := &Instance{ID: "inst-1", m: m}
	srv := &api.ManagedServer{Name: "n1"}

	if !inst.StopServer(t, srv).Shutdown {
		t.Error("stop did not report shutdown")
	}
	if !inst.StartServer(t, srv).Started {
		t.Error("start did not report started")
	}
	if !inst.UpdateServer(t, srv).Updated {
		t.Error("update did not report updated")
	}
	if !inst.ReloadServer(t, srv).Reloaded {
		t.Error("reload did not report reloaded")
	}

	got := f.seen()
	if len(got) != 4 {
		t.Fatalf("the fake received %d requests, want 4", len(got))
	}
	for i, req := range got {
		if req["instance_id"] != "inst-1" {
			t.Errorf("request %d = %v, want instance_id inst-1", i, req)
		}
	}
}

// TestNamedServer creates a server named through WithName.
func TestNamedServer(t *testing.T) {
	client := New(t, testServerURL())
	defer client.Close(t)

	client.WithServer(t, func(t testing.TB, nc *nats.Conn, inst *Instance) {
		if inst.Servers[0].Name != "alpha" {
			t.Fatalf("server name = %q, want alpha", inst.Servers[0].Name)
		}
		if nc.ConnectedServerName() != "alpha" {
			t.Fatalf("connected to %q, want alpha", nc.ConnectedServerName())
		}
	}, WithName("alpha"))
}

// TestNamedCluster creates a cluster named through WithClusterName, with servers
// named through WithServerNames and no count, and one with only the cluster
// named, whose servers take the cluster's name.
func TestNamedCluster(t *testing.T) {
	client := New(t, testServerURL())
	defer client.Close(t)

	client.WithJetStreamCluster(t, 0, func(t testing.TB, nc *nats.Conn, inst *Instance) {
		want := []string{"e1", "e2", "e3"}
		for i, srv := range inst.Servers {
			if srv.Name != want[i] || srv.Cluster != "east" {
				t.Errorf("server %d = %s in %s, want %s in east", i, srv.Name, srv.Cluster, want[i])
			}
		}

		resp, err := nc.Request("$JS.API.INFO", nil, time.Second)
		if err != nil {
			t.Fatalf("jetstream info: %v", err)
		}
		if strings.Contains(string(resp.Data), `"error"`) {
			t.Fatalf("jetstream info: %s", resp.Data)
		}
	}, WithClusterName("east"), WithServerNames("e1", "e2", "e3"))

	client.WithCluster(t, 2, func(t testing.TB, nc *nats.Conn, inst *Instance) {
		for i, srv := range inst.Servers {
			want := []string{"west-n1", "west-n2"}[i]
			if srv.Name != want || srv.Cluster != "west" {
				t.Errorf("server %d = %s in %s, want %s in west", i, srv.Name, srv.Cluster, want)
			}
		}
	}, WithClusterName("west"))
}

// TestNamedSuperCluster creates a super-cluster of clusters of different sizes
// through WithClusters: one named and sized, one with named servers, and one
// left with the default names. A request crosses from one cluster to another,
// so the gateways join them.
func TestNamedSuperCluster(t *testing.T) {
	client := New(t, testServerURL())
	defer client.Close(t)

	clusters := WithClusters(SizedCluster("east", 1), ClusterWithServerNames("west", "w1", "w2"), SizedCluster("", 2))
	client.WithSuperCluster(t, 0, 0, func(t testing.TB, _ *nats.Conn, inst *Instance) {
		if len(inst.Servers) != 5 {
			t.Fatalf("servers = %d, want 5", len(inst.Servers))
		}

		short := inst.ID[len(inst.ID)-8:]
		want := []struct{ name, cluster string }{
			{"east-n1", "east"},
			{"w1", "west"},
			{"w2", "west"},
			{short + "-c3_s1", "SC_" + short + "_3"},
			{short + "-c3_s2", "SC_" + short + "_3"},
		}
		for i, srv := range inst.Servers {
			if srv.Name != want[i].name || srv.Cluster != want[i].cluster {
				t.Errorf("server %d = %s in %s, want %s in %s", i, srv.Name, srv.Cluster, want[i].name, want[i].cluster)
			}
		}

		east, err := nats.Connect(inst.Servers[0].URL)
		if err != nil {
			t.Fatalf("connect east: %v", err)
		}
		defer east.Close()
		west, err := nats.Connect(inst.Servers[2].URL)
		if err != nil {
			t.Fatalf("connect west: %v", err)
		}
		defer west.Close()

		_, err = west.Subscribe("names.echo", func(m *nats.Msg) { _ = m.Respond(m.Data) })
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		err = west.Flush()
		if err != nil {
			t.Fatalf("flush: %v", err)
		}

		deadline := time.Now().Add(10 * time.Second)
		for {
			_, err = east.Request("names.echo", []byte("hi"), 500*time.Millisecond)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("request from east to west: %v", err)
			}
		}
	}, clusters)
}

// TestStopStartByInstanceWithSharedNames creates two clusters holding the same
// cluster name and one server name, a, each with a second server of a name of
// its own. It pins that the bare shared name is refused as ambiguous while the
// per-instance operations, and the Instance helpers, reach the server of their
// own instance only, and that a name the instance does not hold, or an unknown
// instance, stops nothing even when the name is unique.
func TestStopStartByInstanceWithSharedNames(t *testing.T) {
	client := New(t, testServerURL())
	defer client.Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	first := client.CreateCluster(t, 0, false, WithClusterName("shared"), WithServerNames("a", "first-only"))
	defer first.Destroy(t)
	second := client.CreateCluster(t, 0, false, WithClusterName("shared"), WithServerNames("a", "second-only"))
	defer second.Destroy(t)

	running := func(inst *Instance, name string) bool {
		t.Helper()
		for _, srv := range inst.Status(t).Servers {
			if srv.Name == name {
				return srv.Running
			}
		}
		t.Fatalf("instance %s has no server %s", inst.ID, name)
		return false
	}

	_, err := client.m.StopServer(ctx, "a")
	if !errors.Is(err, ErrServerNameAmbiguous) || !errors.Is(err, ErrService) {
		t.Fatalf("stop of the bare shared name: err = %v, want ErrServerNameAmbiguous", err)
	}
	if !strings.Contains(err.Error(), first.ID) || !strings.Contains(err.Error(), second.ID) {
		t.Errorf("ambiguity error %q does not name both instances", err)
	}

	first.StopServer(t, first.Servers[0])
	if running(first, "a") {
		t.Fatal("a of the first instance still runs after its stop")
	}
	if !running(second, "a") {
		t.Fatal("a of the second instance stopped with the first's")
	}

	_, err = client.m.StartInstanceServer(ctx, first.ID, "a")
	if err != nil {
		t.Fatalf("start a of the first instance: %v", err)
	}
	if !running(first, "a") {
		t.Fatal("a of the first instance does not run after its start")
	}

	_, err = client.m.StopInstanceServer(ctx, second.ID, "a")
	if err != nil {
		t.Fatalf("stop a of the second instance: %v", err)
	}
	if running(second, "a") || !running(first, "a") {
		t.Fatal("stop of a in the second instance reached the wrong server")
	}
	second.StartServer(t, second.Servers[0])
	if !running(second, "a") {
		t.Fatal("a of the second instance does not run after its start")
	}

	second.UpdateServer(t, second.Servers[1], WithTopLevel("max_payload: 512KB"))
	second.ReloadServer(t, second.Servers[1])

	_, err = client.m.StopInstanceServer(ctx, "no-such-instance", "a")
	if !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("stop in an unknown instance: err = %v, want ErrInstanceNotFound", err)
	}

	_, err = client.m.StopInstanceServer(ctx, second.ID, "first-only")
	if !errors.Is(err, ErrService) || errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("stop of a name the instance does not hold: err = %v, want ErrService", err)
	}
	_, err = client.m.StopInstanceServer(ctx, "no-such-instance", "first-only")
	if !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("stop of a unique name in an unknown instance: err = %v, want ErrInstanceNotFound", err)
	}
	if !running(first, "first-only") {
		t.Fatal("first-only stopped by a stop naming another instance")
	}
}
