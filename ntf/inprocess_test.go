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
