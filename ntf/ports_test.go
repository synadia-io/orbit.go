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
	"testing"
)

// freeRange finds n consecutive ports nothing on the host holds, so the allocator
// tests run against real binds without depending on fixed ports.
func freeRange(t *testing.T, n int) PortRange {
	t.Helper()

	for range 20 {
		ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4zero})
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		low := ln.Addr().(*net.TCPAddr).Port
		ln.Close()

		if low+n-1 > 65535 {
			continue
		}
		if rangeFree(low, n) {
			return PortRange{Low: low, High: low + n - 1}
		}
	}

	t.Fatalf("found no %d consecutive free ports", n)
	return PortRange{}
}

func rangeFree(low, n int) bool {
	probe := newPortAllocator(PortRange{Low: low, High: low + n - 1})

	for i := range n {
		if !probe.loopbackFree(low + i) {
			return false
		}

		ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4zero, Port: low + i})
		if err != nil {
			return false
		}
		ln.Close()
	}
	return true
}

// mustReserve reserves a port, closes the listener holding it, and returns the port.
func mustReserve(t *testing.T, a *portAllocator) int {
	t.Helper()

	port, ln, err := a.reserve()
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	ln.Close()

	return port
}

// TestPortAllocatorHoldsReservedPort proves reserve hands back a listener that
// holds the port, so nothing else binds it until the caller hands it over.
func TestPortAllocatorHoldsReservedPort(t *testing.T) {
	a := newPortAllocator(freeRange(t, 1))

	port, ln, err := a.reserve()
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	defer ln.Close()

	other, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4zero, Port: port})
	if err == nil {
		other.Close()
		t.Fatalf("port %d could be bound while reserved", port)
	}
}

// TestPortAllocatorSkipsTrackedPorts proves a port handed out is not handed out
// again while tracked, even once its listener is closed, that the range reports
// exhaustion when every port is tracked, and that a released port is handed out
// again.
func TestPortAllocatorSkipsTrackedPorts(t *testing.T) {
	r := freeRange(t, 3)
	a := newPortAllocator(r)

	for i := range 3 {
		got := mustReserve(t, a)
		if got != r.Low+i {
			t.Fatalf("reservation %d got port %d, want %d", i, got, r.Low+i)
		}
	}

	_, _, err := a.reserve()
	if !errors.Is(err, ErrPortRangeExhausted) {
		t.Fatalf("reserve with every port tracked: got %v, want ErrPortRangeExhausted", err)
	}

	a.release(r.Low + 1)

	got := mustReserve(t, a)
	if got != r.Low+1 {
		t.Fatalf("reserve after release got port %d, want %d", got, r.Low+1)
	}
}

// TestPortAllocatorSkipsHeldPorts proves a port another listener holds is skipped,
// and counts as unavailable when the range runs out.
func TestPortAllocatorSkipsHeldPorts(t *testing.T) {
	r := freeRange(t, 2)
	a := newPortAllocator(r)

	held, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4zero, Port: r.Low})
	if err != nil {
		t.Fatalf("hold port %d: %v", r.Low, err)
	}
	defer held.Close()

	got := mustReserve(t, a)
	if got != r.High {
		t.Fatalf("got port %d, want %d past the held port", got, r.High)
	}

	_, _, err = a.reserve()
	if !errors.Is(err, ErrPortRangeExhausted) {
		t.Fatalf("reserve with one port held and one tracked: got %v, want ErrPortRangeExhausted", err)
	}
}

// TestPortAllocatorWraps proves the search continues from where the last one
// stopped rather than rescanning from the bottom, and wraps to the bottom at the
// top of the range.
func TestPortAllocatorWraps(t *testing.T) {
	r := freeRange(t, 3)
	a := newPortAllocator(r)

	first := mustReserve(t, a)
	mustReserve(t, a)
	a.release(first)

	got := mustReserve(t, a)
	if got != r.Low+2 {
		t.Fatalf("got port %d, want %d: the search restarted below the cursor", got, r.Low+2)
	}

	got = mustReserve(t, a)
	if got != first {
		t.Fatalf("got port %d, want %d after wrapping", got, first)
	}
}

// TestPortAllocatorSkipsLoopbackHeldPorts proves a port another listener holds on
// 127.0.0.1 only is skipped. On macOS the wildcard bind succeeds on such a port,
// and connections to localhost would reach the other listener.
func TestPortAllocatorSkipsLoopbackHeldPorts(t *testing.T) {
	r := freeRange(t, 2)
	a := newPortAllocator(r)

	held, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: r.Low})
	if err != nil {
		t.Fatalf("hold port %d on 127.0.0.1: %v", r.Low, err)
	}
	defer held.Close()

	got := mustReserve(t, a)
	if got != r.High {
		t.Fatalf("got port %d, want %d past the port held on 127.0.0.1", got, r.High)
	}

	_, _, err = a.reserve()
	if !errors.Is(err, ErrPortRangeExhausted) {
		t.Fatalf("reserve with one port held on 127.0.0.1 and one tracked: got %v, want ErrPortRangeExhausted", err)
	}
}

func TestResolvePortRange(t *testing.T) {
	ephemeralAt := func(low, high int) func() (int, int, error) {
		return func() (int, int, error) { return low, high, nil }
	}
	unreadable := func() (int, int, error) { return 0, 0, errors.ErrUnsupported }

	defaultRange := PortRange{Low: defaultPortLow, High: defaultPortHigh}

	tests := []struct {
		name      string
		in        PortRange
		ephemeral func() (int, int, error)
		want      PortRange
		wantErr   error
	}{
		{"default below the ephemeral range", PortRange{}, ephemeralAt(49152, 65535), defaultRange, nil},
		{"default above the ephemeral range", PortRange{}, ephemeralAt(1024, 4999), defaultRange, nil},
		{"default trimmed below the ephemeral range", PortRange{}, ephemeralAt(32768, 60999), PortRange{Low: defaultPortLow, High: 32767}, nil},
		{"default with an unreadable ephemeral range", PortRange{}, unreadable, defaultRange, nil},
		{"default the ephemeral range leaves no room for", PortRange{}, ephemeralAt(defaultPortLow+1, 60999), PortRange{}, ErrPortRangeOverlapsEphemeral},
		{"default covered by the ephemeral range", PortRange{}, ephemeralAt(10000, 50000), PortRange{}, ErrPortRangeOverlapsEphemeral},
		{"explicit below the ephemeral range", PortRange{Low: 10000, High: 20000}, ephemeralAt(32768, 60999), PortRange{Low: 10000, High: 20000}, nil},
		{"explicit above the ephemeral range", PortRange{Low: 61000, High: 65000}, ephemeralAt(32768, 60999), PortRange{Low: 61000, High: 65000}, nil},
		{"explicit overlapping the ephemeral range", PortRange{Low: 30000, High: 35000}, ephemeralAt(32768, 60999), PortRange{}, ErrPortRangeOverlapsEphemeral},
		{"explicit ending on the ephemeral start", PortRange{Low: 30000, High: 32768}, ephemeralAt(32768, 60999), PortRange{}, ErrPortRangeOverlapsEphemeral},
		{"explicit starting on the ephemeral end", PortRange{Low: 60999, High: 65000}, ephemeralAt(32768, 60999), PortRange{}, ErrPortRangeOverlapsEphemeral},
		{"explicit with an unreadable ephemeral range", PortRange{Low: 30000, High: 35000}, unreadable, PortRange{Low: 30000, High: 35000}, nil},
		{"low of zero", PortRange{Low: 0, High: 100}, ephemeralAt(49152, 65535), PortRange{}, ErrInvalidPortRange},
		{"high above 65535", PortRange{Low: 100, High: 65536}, ephemeralAt(49152, 65535), PortRange{}, ErrInvalidPortRange},
		{"low above high", PortRange{Low: 200, High: 100}, ephemeralAt(49152, 65535), PortRange{}, ErrInvalidPortRange},
		{"low equal to high", PortRange{Low: 100, High: 100}, ephemeralAt(49152, 65535), PortRange{}, ErrInvalidPortRange},
		{"only low set", PortRange{Low: 100}, ephemeralAt(49152, 65535), PortRange{}, ErrInvalidPortRange},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolvePortRange(tt.in, tt.ephemeral, slog.New(slog.DiscardHandler))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("range = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestNewRejectsInvalidPortRange proves New validates Options.PortRange before
// starting anything.
func TestNewRejectsInvalidPortRange(t *testing.T) {
	svc, err := New(t.Context(), Options{Dir: t.TempDir(), PortRange: PortRange{Low: 5000, High: 4000}})
	if err == nil {
		svc.Close()
		t.Fatal("New accepted an inverted port range")
	}
	if !errors.Is(err, ErrInvalidPortRange) {
		t.Fatalf("New error = %v, want ErrInvalidPortRange", err)
	}
}

// TestEphemeralPortRange checks the host lookup returns a plausible range where
// it is supported. It skips where the range cannot be read, as New does.
func TestEphemeralPortRange(t *testing.T) {
	low, high, err := ephemeralPortRange()
	if err != nil {
		t.Skipf("ephemeral port range not readable here: %v", err)
	}
	if low < 1 || high > 65535 || low > high {
		t.Fatalf("ephemeral port range %d-%d is not a port range", low, high)
	}
}
