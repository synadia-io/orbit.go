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
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"testing"
)

// placementService returns a Service with just enough state for an in-process
// placement: a directory, a port allocator over r, an empty instance table and
// the in-process backend. It starts no server.
func placementService(t *testing.T, r PortRange) *Service {
	t.Helper()

	s := &Service{
		log:       slog.New(slog.DiscardHandler),
		dir:       t.TempDir(),
		ports:     newPortAllocator(r),
		instances: map[string]*instance{},
	}
	s.backend = newInProcessBackend(s)

	return s
}

// placeInstance returns the in-process placement of inst, which s must have
// registered.
func placeInstance(t *testing.T, s *Service, inst *instance) *inProcessPlacement {
	t.Helper()

	place, err := s.backend.Placement(t.Context(), inst.ID)
	if err != nil {
		t.Fatalf("Placement: %v", err)
	}

	return place.(*inProcessPlacement)
}

// TestInProcessPlacementListenerAddress proves the in-process placement hands
// out distinct ports from the configured range on localhost, holds each one, and
// records them on the instance.
func TestInProcessPlacementListenerAddress(t *testing.T) {
	r := freeRange(t, 4)
	s := placementService(t, r)
	inst := s.newInstance("cluster", "")
	p := placeInstance(t, s, inst)
	t.Cleanup(func() { s.dropInstance(inst) })

	names := []string{"client", "cluster", "websocket"}
	var ports []int
	for _, name := range names {
		node := shortID(inst.ID) + "-n1"
		l, err := p.ListenerAddress(node, name)
		if err != nil {
			t.Fatalf("ListenerAddress %s: %v", name, err)
		}
		ln := p.rec.held[[2]string{node, name}]
		if ln == nil {
			t.Fatalf("listener %s is not held", name)
		}

		if l.Name != name {
			t.Errorf("listener name = %q, want %q", l.Name, name)
		}
		if l.Endpoint.Host != "localhost" {
			t.Errorf("listener %s host = %q, want localhost", name, l.Endpoint.Host)
		}
		if l.Endpoint.Port < r.Low || l.Endpoint.Port > r.High {
			t.Errorf("listener %s port %d outside range %d-%d", name, l.Endpoint.Port, r.Low, r.High)
		}
		if ln.Addr().(*net.TCPAddr).Port != l.Endpoint.Port {
			t.Errorf("listener %s held on port %d, want %d", name, ln.Addr().(*net.TCPAddr).Port, l.Endpoint.Port)
		}
		if slices.Contains(ports, l.Endpoint.Port) {
			t.Errorf("listener %s port %d handed out twice", name, l.Endpoint.Port)
		}
		ports = append(ports, l.Endpoint.Port)
	}

	s.mu.Lock()
	recorded := slices.Clone(inst.ports)
	s.mu.Unlock()

	if !slices.Equal(recorded, ports) {
		t.Errorf("instance ports = %v, want %v", recorded, ports)
	}
}

// TestInProcessPlacementReleasesOnTeardown proves the ports the in-process
// placement hands out return to the range and their listeners are closed when
// the instance is torn down, and that a port handed out after a concurrent
// destroy is given back, and its listener closed, at once.
func TestInProcessPlacementReleasesOnTeardown(t *testing.T) {
	s := placementService(t, freeRange(t, 4))
	inst := s.newInstance("server", "")
	p := placeInstance(t, s, inst)

	node := shortID(inst.ID) + "-n1"
	l, err := p.ListenerAddress(node, "client")
	if err != nil {
		t.Fatalf("ListenerAddress: %v", err)
	}
	ln := p.rec.held[[2]string{node, "client"}]

	s.dropInstance(inst)

	if inst.ports != nil {
		t.Errorf("instance ports = %v after teardown, want none", inst.ports)
	}
	if !portFree(s.ports, l.Endpoint.Port) {
		t.Errorf("port %d still in use after teardown", l.Endpoint.Port)
	}
	err = ln.Close()
	if !errors.Is(err, net.ErrClosed) {
		t.Errorf("listener on port %d left open after teardown", l.Endpoint.Port)
	}

	late, err := p.ListenerAddress(node, "cluster")
	if err != nil {
		t.Fatalf("ListenerAddress after teardown: %v", err)
	}
	if p.rec.held != nil {
		t.Errorf("listener of a late reservation kept after teardown")
	}
	if !portBindable(late.Endpoint.Port) {
		t.Errorf("listener of port %d reserved after teardown left open", late.Endpoint.Port)
	}

	if inst.ports != nil {
		t.Errorf("instance ports = %v after a late reservation, want none", inst.ports)
	}
	if !portFree(s.ports, late.Endpoint.Port) {
		t.Errorf("port %d reserved after teardown was not given back", late.Endpoint.Port)
	}
}

// TestInProcessPlacementDirectories proves the in-process placement returns
// today's directory layout: <Dir>/<id> for the instance, and n<i> or c<c>_s<i>
// under it as a node's store directory, which is the node's directory.
func TestInProcessPlacementDirectories(t *testing.T) {
	s := placementService(t, freeRange(t, 1))
	inst := s.newInstance("super-cluster", "")
	p := placeInstance(t, s, inst)
	short := shortID(inst.ID)
	root := filepath.Join(s.Dir(), inst.ID)

	if p.InstanceDir() != root {
		t.Errorf("InstanceDir = %q, want %q", p.InstanceDir(), root)
	}

	cases := map[string]string{
		short + "-n1":    filepath.Join(root, "n1"),
		short + "-n3":    filepath.Join(root, "n3"),
		short + "-c1_s1": filepath.Join(root, "c1_s1"),
		short + "-c2_s3": filepath.Join(root, "c2_s3"),
	}
	for node, want := range cases {
		got := p.StoreDir(node)
		if got != want {
			t.Errorf("StoreDir(%q) = %q, want %q", node, got, want)
		}
	}
}

// portFree reports whether a no longer tracks port as handed out.
func portFree(a *portAllocator, port int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, inUse := a.inUse[port]
	return !inUse
}
