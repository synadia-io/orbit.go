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
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/backend"
)

// portBindable reports whether port can be bound on the wildcard address, which
// it cannot while a managed server or a reservation listens on it.
func portBindable(port int) bool {
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4zero, Port: port})
	if err != nil {
		return false
	}
	ln.Close()

	return true
}

// heldListeners returns the listeners p's instance still holds.
func heldListeners(p *inProcessPlacement) []*net.TCPListener {
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()

	var held []*net.TCPListener
	for _, ln := range p.rec.held {
		held = append(held, ln)
	}

	return held
}

// TestBackendTeardownClosesHeldListenersOfFailedPlan proves the listeners
// placement holds for a plan that fails part way are closed, and their ports
// released, by the teardown a failed create runs.
func TestBackendTeardownClosesHeldListenersOfFailedPlan(t *testing.T) {
	s := placementService(t, freeRange(t, 8))
	inst := s.newInstance("cluster", "")
	p := placeInstance(t, s, inst)

	spec := instanceSpec{Kind: "cluster", Servers: 3, MainTemplate: "{{ if eq .ServerIndex 2 }}{{ .NoSuchField }}{{ end }}"}
	_, err := planInstance(spec, inst.ID, p, server.VERSION)
	if err == nil {
		t.Fatal("planInstance succeeded, want an error")
	}

	held := heldListeners(p)
	if len(held) != 6 {
		t.Fatalf("placement holds %d listeners, want 6", len(held))
	}

	s.dropInstance(inst)

	for _, ln := range held {
		err := ln.Close()
		if !errors.Is(err, net.ErrClosed) {
			t.Errorf("listener %s left open after teardown", ln.Addr())
		}
	}
	if trackedPorts(s) != 0 {
		t.Errorf("%d ports still reserved after teardown", trackedPorts(s))
	}
}

// TestBackendProvisionHandsOffHeldListeners proves Provision hands each node the
// listeners holding its ports as it starts it, so the node binds the planned
// ports, and that the node's addresses carry those ports with the internal host
// on 127.0.0.1. Teardown then stops the node and releases its ports.
func TestBackendProvisionHandsOffHeldListeners(t *testing.T) {
	s := placementService(t, freeRange(t, 4))
	inst := s.newInstance("server", "")
	p := placeInstance(t, s, inst)

	spec := instanceSpec{Kind: "server", Servers: 1, MainTemplate: serverConfigTemplate}
	plan, err := planInstance(spec, inst.ID, p, s.backend.DefaultVersion())
	if err != nil {
		t.Fatalf("planInstance: %v", err)
	}
	t.Cleanup(func() { s.dropInstance(inst) })

	nodes, err := s.backend.Provision(t.Context(), &plan.InstancePlan)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}

	if len(heldListeners(p)) != 0 {
		t.Errorf("placement still holds %d listeners after Provision", len(heldListeners(p)))
	}
	if len(nodes) != 1 || nodes[0].Name() != plan.Nodes[0].Name {
		t.Fatalf("Provision returned %d nodes, want the one node %s", len(nodes), plan.Nodes[0].Name)
	}

	want := listenerOf(plan.nodes[0], "client").Endpoint.Port
	addrs := nodes[0].Addresses()
	if len(addrs) != 1 || addrs[0].Name != "client" {
		t.Fatalf("addresses = %+v, want one client address", addrs)
	}
	if addrs[0].Internal != (backend.Endpoint{Host: "127.0.0.1", Port: want}) || addrs[0].External.Port != want {
		t.Errorf("client address = %+v, want port %d, internally on 127.0.0.1", addrs[0], want)
	}
	if portBindable(want) {
		t.Errorf("client port %d is not bound by the started node", want)
	}

	s.dropInstance(inst)

	if !portBindable(want) {
		t.Errorf("client port %d still bound after teardown", want)
	}
	if trackedPorts(s) != 0 {
		t.Errorf("%d ports still reserved after teardown", trackedPorts(s))
	}
}

// TestBackendProvisionFailsPartWay takes the client port of a cluster's second
// node before it starts, and proves Provision stops the node it already started
// and fails with the create's 003 answer, wrapping ErrServerListen, and that the
// teardown a failed create runs closes the third node's held listeners and
// releases every port.
func TestBackendProvisionFailsPartWay(t *testing.T) {
	s := placementService(t, freeRange(t, 8))
	inst := s.newInstance("cluster", "")
	p := placeInstance(t, s, inst)

	spec := instanceSpec{Kind: "cluster", Servers: 3, MainTemplate: serverConfigTemplate}
	plan, err := planInstance(spec, inst.ID, p, s.backend.DefaultVersion())
	if err != nil {
		t.Fatalf("planInstance: %v", err)
	}

	calls := takePortHook(t, func(call int) bool { return call == 2 })

	_, err = s.backend.Provision(t.Context(), &plan.InstancePlan)
	if err == nil {
		s.dropInstance(inst)
		t.Fatal("Provision succeeded, want an error")
	}
	if !errors.Is(err, ErrServerListen) {
		t.Errorf("error %v does not wrap ErrServerListen", err)
	}
	code, msg := backendErrorResponse(err)
	if code != "003" || !strings.HasPrefix(msg, "Server creation failed: ") {
		t.Errorf("answered %s %q, want 003 Server creation failed", code, msg)
	}
	if calls.Load() != 2 {
		t.Errorf("started %d nodes, want 2", calls.Load())
	}

	first := listenerOf(plan.nodes[0], "client").Endpoint.Port
	if !portBindable(first) {
		t.Errorf("first node still listens on %d after the failed Provision", first)
	}

	held := heldListeners(p)
	if len(held) != 2 {
		t.Errorf("placement holds %d listeners, want the third node's 2", len(held))
	}

	s.dropInstance(inst)

	for _, ln := range held {
		err := ln.Close()
		if !errors.Is(err, net.ErrClosed) {
			t.Errorf("listener %s left open after teardown", ln.Addr())
		}
	}
	if trackedPorts(s) != 0 {
		t.Errorf("%d ports still reserved after teardown", trackedPorts(s))
	}
}

// provisionServer plans and provisions a single server with snippets on its own
// service, and returns its plan and node. The instance is torn down when the
// test ends.
func provisionServer(t *testing.T, snippets map[string]string) (*instancePlan, *inProcessNode) {
	t.Helper()

	s := placementService(t, freeRange(t, 4))
	inst := s.newInstance("server", "")
	p := placeInstance(t, s, inst)
	t.Cleanup(func() { s.dropInstance(inst) })

	spec := instanceSpec{Kind: "server", Servers: 1, Snippets: snippets, MainTemplate: serverConfigTemplate}
	plan, err := planInstance(spec, inst.ID, p, s.backend.DefaultVersion())
	if err != nil {
		t.Fatalf("planInstance: %v", err)
	}

	nodes, err := s.backend.Provision(t.Context(), &plan.InstancePlan)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}

	return plan, nodes[0].(*inProcessNode)
}

// TestInProcessApplyRefusesIdentityChange proves Apply refuses a revision that
// changes any part of the node's identity, naming the part.
func TestInProcessApplyRefusesIdentityChange(t *testing.T) {
	plan, node := provisionServer(t, nil)

	tests := []struct {
		field  string
		change func(n *backend.NodePlan)
	}{
		{"name", func(n *backend.NodePlan) { n.Name += "x" }},
		{"dir", func(n *backend.NodePlan) { n.Dir = "n2" }},
		{"cluster", func(n *backend.NodePlan) { n.Cluster = "C_other" }},
		{"listeners", func(n *backend.NodePlan) {
			n.Listeners = append(slices.Clone(n.Listeners), backend.Listener{Name: "websocket", Endpoint: backend.Endpoint{Host: "localhost", Port: 1}})
		}},
		{"persistent", func(n *backend.NodePlan) { n.Persistent = []string{"/elsewhere"} }},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			rev := *plan.Nodes[0]
			tt.change(&rev)

			err := node.Apply(t.Context(), &rev)
			if !errors.Is(err, errIdentityChange) {
				t.Fatalf("Apply = %v, want an identity change", err)
			}
			if !strings.Contains(err.Error(), tt.field) {
				t.Errorf("error %q does not name %s", err, tt.field)
			}
		})
	}
}

// TestInProcessApplyRefusesVersionChange proves Apply refuses a revision that
// changes the version with ErrUnsupported, since the in-process backend does
// not declare version-change.
func TestInProcessApplyRefusesVersionChange(t *testing.T) {
	plan, node := provisionServer(t, nil)

	rev := *plan.Nodes[0]
	rev.Version = "0.0.1"

	err := node.Apply(t.Context(), &rev)
	if !errors.Is(err, backend.ErrUnsupported) {
		t.Errorf("Apply = %v, want ErrUnsupported", err)
	}
}

// TestInProcessApplyWritesConfigAndKeepsFiles proves Apply overwrites the node's
// config, writes the revision's files with their modes, and keeps a file the
// revision does not carry.
func TestInProcessApplyWritesConfigAndKeepsFiles(t *testing.T) {
	const top = "# top as created\n"
	plan, node := provisionServer(t, map[string]string{"top": top})

	rev := *plan.Nodes[0]
	rev.Config = []byte("# the revised config\n")
	rev.Files = []backend.File{{Path: filepath.Join(rev.Dir, "snippets", "accounts.conf"), Data: []byte("# accounts\n"), Mode: 0600}}

	err := node.Apply(t.Context(), &rev)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	cfg, err := os.ReadFile(node.cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if string(cfg) != string(rev.Config) {
		t.Errorf("config = %q, want %q", cfg, rev.Config)
	}

	path := filepath.Join(node.instDir, rev.Files[0].Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read revision file: %v", err)
	}
	if string(data) != "# accounts\n" {
		t.Errorf("revision file = %q, want %q", data, "# accounts\n")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat revision file: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("revision file mode = %v, want 0600", info.Mode().Perm())
	}

	kept, err := os.ReadFile(filepath.Join(node.instDir, rev.Dir, "snippets", "top.conf"))
	if err != nil {
		t.Fatalf("read kept file: %v", err)
	}
	if string(kept) != top {
		t.Errorf("kept file = %q, want %q", kept, top)
	}
}

// TestInProcessApplyRefusesPathOutsideNode proves Apply refuses a revision with
// a file outside the node's directory, naming the path, and writes nothing: the
// config and an existing file are unchanged, and neither the revision's valid
// file nor the refused one appears.
func TestInProcessApplyRefusesPathOutsideNode(t *testing.T) {
	const top = "# top as created\n"

	tests := []struct {
		name string
		path string
	}{
		{"escapes the instance", filepath.Join("..", "x")},
		{"absolute", filepath.Join(t.TempDir(), "x")},
		{"another node", filepath.Join("n2", "snippets", "x.conf")},
		{"sibling prefix", filepath.Join("n1x", "snippets", "x.conf")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, node := provisionServer(t, map[string]string{"top": top})

			cfgBefore, err := os.ReadFile(node.cfgPath)
			if err != nil {
				t.Fatalf("read config: %v", err)
			}

			rev := *plan.Nodes[0]
			rev.Config = []byte("# the revised config\n")
			valid := filepath.Join(rev.Dir, "snippets", "accounts.conf")
			rev.Files = []backend.File{
				{Path: valid, Data: []byte("# accounts\n"), Mode: 0600},
				{Path: tt.path, Data: []byte("# outside\n"), Mode: 0600},
			}

			err = node.Apply(t.Context(), &rev)
			if !errors.Is(err, errPathOutsideNode) {
				t.Fatalf("Apply = %v, want a path outside the node", err)
			}
			if !strings.Contains(err.Error(), tt.path) {
				t.Errorf("error %q does not name %s", err, tt.path)
			}

			cfg, err := os.ReadFile(node.cfgPath)
			if err != nil {
				t.Fatalf("read config: %v", err)
			}
			if string(cfg) != string(cfgBefore) {
				t.Errorf("config = %q, want it unchanged %q", cfg, cfgBefore)
			}

			kept, err := os.ReadFile(filepath.Join(node.instDir, rev.Dir, "snippets", "top.conf"))
			if err != nil {
				t.Fatalf("read existing file: %v", err)
			}
			if string(kept) != top {
				t.Errorf("existing file = %q, want %q", kept, top)
			}

			absent := []string{filepath.Join(node.instDir, valid), filepath.Join(node.instDir, tt.path)}
			if filepath.IsAbs(tt.path) {
				absent = append(absent, tt.path)
			}
			for _, path := range absent {
				_, err := os.Stat(path)
				if !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("%s exists after a refused Apply: %v", path, err)
				}
			}
		})
	}
}

// TestStartRacedByDestroyAnswersNotFound stalls a tester.start.server of a
// stopped server just before its server starts, destroys the instance
// meanwhile, and proves the destroy waits for the start, the start answers 404,
// no server is left running and the instance directory is gone.
func TestStartRacedByDestroyAnswersNotFound(t *testing.T) {
	// Cleanups run in reverse: the stalled start is released, the service
	// closes, the request goroutines return, and only then is the hook reset,
	// when nothing can read it any more.
	t.Cleanup(func() { beforeServerStart = nil })

	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)

	svc := startTestService(t)

	stalled := make(chan struct{})
	release := make(chan struct{})
	var stallOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{}, &created)
	name := created.Servers[0].Name
	port := created.Servers[0].Port

	svc.mu.Lock()
	dir := svc.instances[created.ID].RootDir
	svc.mu.Unlock()

	node := svc.findServerByName(name).backendNode.(*inProcessNode)
	nodeTorn := func() bool {
		node.mu.Lock()
		defer node.mu.Unlock()

		return node.torn
	}

	mustRequest(t, svc.nc, "tester.stop.server", api.StopServerRequest{Name: name}, nil)

	beforeServerStart = func(*server.Options) {
		stallOnce.Do(func() { close(stalled) })
		<-release
	}

	type reply struct {
		msg *nats.Msg
		err error
	}
	request := func(subject string, req any) <-chan reply {
		payload, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal %s: %v", subject, err)
		}

		replies := make(chan reply, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg, err := svc.nc.Request(subject, payload, 30*time.Second)
			replies <- reply{msg: msg, err: err}
		}()

		return replies
	}
	await := func(subject string, replies <-chan reply) *nats.Msg {
		select {
		case r := <-replies:
			if r.err != nil {
				t.Fatalf("request %s: %v", subject, r.err)
			}
			return r.msg
		case <-time.After(40 * time.Second):
			t.Fatalf("request %s never returned", subject)
			return nil
		}
	}

	starts := request("tester.start.server", api.StartServerRequest{Name: name})

	select {
	case <-stalled:
	case <-time.After(30 * time.Second):
		t.Fatal("the start never reached its server start")
	}

	destroys := request("tester.destroy", api.DestroyRequest{InstanceID: created.ID})

	// The destroy marks the node torn, then waits for the stalled start.
	deadline := time.Now().Add(30 * time.Second)
	for !nodeTorn() {
		if time.Now().After(deadline) {
			t.Fatal("the destroy never marked the node torn")
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case <-destroys:
		t.Fatal("the destroy answered while the start was in flight")
	default:
	}

	unblock()

	msg := await("tester.start.server", starts)
	code := msg.Header.Get("Nats-Service-Error-Code")
	desc := msg.Header.Get("Nats-Service-Error")
	if code != "404" {
		t.Errorf("start answered %q %q, want 404", code, desc)
	}

	msg = await("tester.destroy", destroys)
	if e := msg.Header.Get("Nats-Service-Error"); e != "" {
		t.Fatalf("destroy failed: %s", e)
	}
	var destroyed api.DestroyResponse
	err := json.Unmarshal(msg.Data, &destroyed)
	if err != nil {
		t.Fatalf("unmarshal destroy response: %v", err)
	}
	if !destroyed.Destroyed {
		t.Error("destroy reported nothing destroyed")
	}

	if !portBindable(port) {
		t.Errorf("a server still listens on %d", port)
	}
	_, err = os.Stat(dir)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("instance dir %s left behind (stat err %v)", dir, err)
	}
}

// TestCreateRacedByResetAnswersNotFound stalls a JetStream cluster create while
// its second node starts, resets the service meanwhile, and proves the create
// answers 404, leaves no server running, no port reserved and no instance
// directory, and registers nothing.
func TestCreateRacedByResetAnswersNotFound(t *testing.T) {
	svc := startTestService(t)

	stalled := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var started []int

	beforeServerStart = func(opts *server.Options) {
		mu.Lock()
		started = append(started, opts.Port)
		call := len(started)
		mu.Unlock()

		if call == 2 {
			close(stalled)
			<-release
		}
	}
	t.Cleanup(func() { beforeServerStart = nil })

	payload, err := json.Marshal(api.CreateClusterRequest{Servers: 3, JetStream: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	replies := make(chan *nats.Msg, 1)
	go func() {
		msg, err := svc.nc.Request("tester.create.cluster", payload, 30*time.Second)
		if err != nil {
			t.Errorf("request: %v", err)
		}
		replies <- msg
	}()

	select {
	case <-stalled:
	case <-time.After(30 * time.Second):
		t.Fatal("the create never started its second node")
	}

	// The reset waits for the stalled Provision, so it runs alongside. The start
	// is released once the reset has marked the instance torn.
	resets := make(chan error, 1)
	go func() { resets <- svc.Reset() }()

	b := svc.backend.(*inProcessBackend)
	tornDir := func() string {
		b.mu.Lock()
		defer b.mu.Unlock()

		for _, rec := range b.instances {
			if rec.torn {
				return rec.inst.RootDir
			}
		}
		return ""
	}

	deadline := time.Now().Add(30 * time.Second)
	dir := tornDir()
	for dir == "" {
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("the reset never marked the instance torn")
		}
		time.Sleep(10 * time.Millisecond)
		dir = tornDir()
	}
	close(release)

	msg := <-replies
	err = <-resets
	if err != nil {
		t.Errorf("Reset: %v", err)
	}
	if msg == nil {
		t.FailNow()
	}

	code := msg.Header.Get("Nats-Service-Error-Code")
	desc := msg.Header.Get("Nats-Service-Error")
	if code != "404" || desc != "Instance not found" {
		t.Errorf("answered %q %q, want 404 Instance not found", code, desc)
	}

	mu.Lock()
	ports := started
	mu.Unlock()
	if len(ports) != 2 {
		t.Errorf("started %d servers, want 2", len(ports))
	}
	for _, port := range ports {
		if !portBindable(port) {
			t.Errorf("a server still listens on %d", port)
		}
	}
	if trackedPorts(svc) != 0 {
		t.Errorf("%d ports still reserved", trackedPorts(svc))
	}
	_, err = os.Stat(dir)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("instance dir %s left behind (stat err %v)", dir, err)
	}

	var list api.ListResponse
	mustRequest(t, svc.nc, "tester.list", nil, &list)
	if len(list.Instances) != 0 {
		t.Errorf("%d instances registered, want none", len(list.Instances))
	}
}
