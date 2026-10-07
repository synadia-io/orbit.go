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
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"testing"
)

// placementService returns a Service with just enough state for an in-process
// placement: a directory, a port allocator over r and an empty instance table.
// It starts no server.
func placementService(t *testing.T, r PortRange) *Service {
	t.Helper()

	return &Service{
		log:       slog.New(slog.DiscardHandler),
		dir:       t.TempDir(),
		ports:     newPortAllocator(r),
		instances: map[string]*instance{},
	}
}

// TestInProcessPlacementListenerAddress proves the in-process placement hands
// out distinct ports from the configured range on localhost, holds each one, and
// records them on the instance.
func TestInProcessPlacementListenerAddress(t *testing.T) {
	r := freeRange(t, 4)
	s := placementService(t, r)
	inst := s.newInstance("cluster", "")
	p := s.newInProcessPlacement(inst)

	names := []string{"client", "cluster", "websocket"}
	var ports []int
	for _, name := range names {
		l, ln, err := p.listenerAddress(shortID(inst.ID)+"-n1", name)
		if err != nil {
			t.Fatalf("listenerAddress %s: %v", name, err)
		}
		defer ln.Close()

		if l.Name != name {
			t.Errorf("listener name = %q, want %q", l.Name, name)
		}
		if l.Host != "localhost" {
			t.Errorf("listener %s host = %q, want localhost", name, l.Host)
		}
		if l.Port < r.Low || l.Port > r.High {
			t.Errorf("listener %s port %d outside range %d-%d", name, l.Port, r.Low, r.High)
		}
		if ln.Addr().(*net.TCPAddr).Port != l.Port {
			t.Errorf("listener %s held on port %d, want %d", name, ln.Addr().(*net.TCPAddr).Port, l.Port)
		}
		if slices.Contains(ports, l.Port) {
			t.Errorf("listener %s port %d handed out twice", name, l.Port)
		}
		ports = append(ports, l.Port)
	}

	s.mu.Lock()
	recorded := slices.Clone(inst.ports)
	s.mu.Unlock()

	if !slices.Equal(recorded, ports) {
		t.Errorf("instance ports = %v, want %v", recorded, ports)
	}
}

// TestInProcessPlacementReleasesOnTeardown proves the ports the in-process
// placement hands out return to the range when the instance is torn down, and
// that a port handed out after a concurrent destroy is given back at once.
func TestInProcessPlacementReleasesOnTeardown(t *testing.T) {
	s := placementService(t, freeRange(t, 4))
	inst := s.newInstance("server", "")
	p := s.newInProcessPlacement(inst)

	l, ln, err := p.listenerAddress(shortID(inst.ID)+"-n1", "client")
	if err != nil {
		t.Fatalf("listenerAddress: %v", err)
	}
	ln.Close()

	s.dropInstance(inst.ID)

	if inst.ports != nil {
		t.Errorf("instance ports = %v after teardown, want none", inst.ports)
	}
	if !portFree(s.ports, l.Port) {
		t.Errorf("port %d still in use after teardown", l.Port)
	}

	late, ln, err := p.listenerAddress(shortID(inst.ID)+"-n1", "cluster")
	if err != nil {
		t.Fatalf("listenerAddress after teardown: %v", err)
	}
	ln.Close()

	if inst.ports != nil {
		t.Errorf("instance ports = %v after a late reservation, want none", inst.ports)
	}
	if !portFree(s.ports, late.Port) {
		t.Errorf("port %d reserved after teardown was not given back", late.Port)
	}
}

// TestInProcessPlacementDirectories proves the in-process placement returns
// today's directory layout: <Dir>/<id> for the instance, and n<i> or c<c>_s<i>
// under it for a node.
func TestInProcessPlacementDirectories(t *testing.T) {
	s := placementService(t, freeRange(t, 1))
	inst := s.newInstance("super-cluster", "")
	p := s.newInProcessPlacement(inst)
	short := shortID(inst.ID)
	root := filepath.Join(s.Dir(), inst.ID)

	if p.instanceDir() != root {
		t.Errorf("instanceDir = %q, want %q", p.instanceDir(), root)
	}

	cases := map[string]string{
		short + "-n1":    filepath.Join(root, "n1"),
		short + "-n3":    filepath.Join(root, "n3"),
		short + "-c1_s1": filepath.Join(root, "c1_s1"),
		short + "-c2_s3": filepath.Join(root, "c2_s3"),
	}
	for node, want := range cases {
		got := p.nodeDir(node)
		if got != want {
			t.Errorf("nodeDir(%q) = %q, want %q", node, got, want)
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
