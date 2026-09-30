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
	"fmt"
	"log/slog"
	"net"
	"sync"
)

// resolvePortRange validates the configured managed server port range and fits it
// to the host. The zero value means the default range. A range that does not
// intersect the OS ephemeral port range is used as is. When the ephemeral range
// starts inside the default range, the default is trimmed to end below it; any
// other overlap, and any overlap of a range the caller set, is an error.
// ephemeralRange reports the low and high ends of the ephemeral range; when it
// fails the overlap check is skipped.
func resolvePortRange(r PortRange, ephemeralRange func() (int, int, error), log *slog.Logger) (PortRange, error) {
	explicit := r != PortRange{}
	if !explicit {
		r = PortRange{Low: defaultPortLow, High: defaultPortHigh}
	}

	if r.Low < 1 || r.High > 65535 || r.Low >= r.High {
		return PortRange{}, fmt.Errorf("%w: %d-%d", ErrInvalidPortRange, r.Low, r.High)
	}

	ephemeralLow, ephemeralHigh, err := ephemeralRange()
	if err != nil {
		log.Debug("Could not read the OS ephemeral port range, skipping the overlap check", "err", err)
		return r, nil
	}
	if r.High < ephemeralLow || r.Low > ephemeralHigh {
		return r, nil
	}

	// Trimming needs the ephemeral range to start inside the default range and
	// leave at least two ports below it.
	if explicit || ephemeralLow <= r.Low+1 {
		return PortRange{}, fmt.Errorf("%w: %d-%d, ephemeral ports are %d-%d", ErrPortRangeOverlapsEphemeral, r.Low, r.High, ephemeralLow, ephemeralHigh)
	}

	trimmed := PortRange{Low: r.Low, High: ephemeralLow - 1}
	log.Info("Trimmed the managed server port range to end below the OS ephemeral range", "low", trimmed.Low, "high", trimmed.High, "ephemeral_low", ephemeralLow, "ephemeral_high", ephemeralHigh)

	return trimmed, nil
}

// portAllocator hands out the TCP ports managed servers listen on. It tracks the
// ports it handed out until they are released and test-binds every candidate, so
// it hands out neither a port a server of this service owns nor one another
// program holds.
type portAllocator struct {
	low  int
	high int

	mu sync.Mutex
	// next is the port the next search starts at. It walks the range and wraps,
	// so successive reservations do not rescan the ports handed out before them.
	next  int
	inUse map[int]struct{}

	// ipv6Loopback reports whether the host has an IPv6 loopback address to
	// probe candidates on.
	ipv6Loopback bool
}

func newPortAllocator(r PortRange) *portAllocator {
	a := &portAllocator{
		low:   r.Low,
		high:  r.High,
		next:  r.Low,
		inUse: map[int]struct{}{},
	}

	ln, err := net.ListenTCP("tcp6", &net.TCPAddr{IP: net.IPv6loopback})
	if err == nil {
		ln.Close()
		a.ipv6Loopback = true
	}

	return a
}

// reserve returns a free port from the range and the still-open listener holding
// it on the wildcard address (0.0.0.0), the address managed servers bind. A port
// another program listens on is skipped. A wildcard bind alone does not find all
// of those: on macOS it succeeds while another process listens on the port on a
// loopback address, and connections to localhost then reach that process. So each
// candidate is first probed on 127.0.0.1 and, where the host has one, on ::1,
// closing each probe at once, before the wildcard bind. The caller must close the
// listener immediately before the server binds the port for real, and release the
// port once no server will bind it again.
func (a *portAllocator) reserve() (int, *net.TCPListener, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for range a.high - a.low + 1 {
		port := a.next
		a.next++
		if a.next > a.high {
			a.next = a.low
		}

		_, taken := a.inUse[port]
		if taken {
			continue
		}

		if !a.loopbackFree(port) {
			continue
		}

		ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4zero, Port: port})
		if err != nil {
			continue
		}

		a.inUse[port] = struct{}{}
		return port, ln, nil
	}

	return 0, nil, fmt.Errorf("%w: %d-%d", ErrPortRangeExhausted, a.low, a.high)
}

// loopbackFree reports whether port can be bound on the IPv4 loopback address
// and, where the host has one, the IPv6 loopback address. Each probe is closed at
// once.
func (a *portAllocator) loopbackFree(port int) bool {
	addrs := []*net.TCPAddr{{IP: net.IPv4(127, 0, 0, 1), Port: port}}
	if a.ipv6Loopback {
		addrs = append(addrs, &net.TCPAddr{IP: net.IPv6loopback, Port: port})
	}

	for _, addr := range addrs {
		ln, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return false
		}
		ln.Close()
	}

	return true
}

// release returns ports to the range.
func (a *portAllocator) release(ports ...int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, port := range ports {
		delete(a.inUse, port)
	}
}
