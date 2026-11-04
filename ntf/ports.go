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

// osPortTries bounds how many ports chooseOSPort draws from the OS. A draw lands
// on a port held on a loopback address about 0.13% of the time on a host with
// such listeners, so a second draw nearly always clears it. Draws are not
// independent: macOS hands out ports in order, so a run of held ports costs a try
// each, and only a run this long directly ahead of its next port uses them all.
const osPortTries = 32

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

// loopbackFree reports whether port can be bound on the IPv4 loopback address, the
// IPv4 wildcard address and, where the host has one, the IPv6 loopback address.
// The wildcard probe is there for macOS, where a listener on the IPv4 wildcard
// answers 127.0.0.1 yet does not stop a bind to 127.0.0.1 or a dual-stack bind.
// Each probe is closed at once.
func (a *portAllocator) loopbackFree(port int) bool {
	type probe struct {
		network string
		ip      net.IP
	}

	probes := []probe{{"tcp4", net.IPv4(127, 0, 0, 1)}, {"tcp4", net.IPv4zero}}
	if a.ipv6Loopback {
		probes = append(probes, probe{"tcp6", net.IPv6loopback})
	}

	for _, p := range probes {
		ln, err := net.ListenTCP(p.network, &net.TCPAddr{IP: p.ip, Port: port})
		if err != nil {
			return false
		}
		ln.Close()
	}

	return true
}

// chooseOSPort returns a port from draw that is clear on the loopback addresses,
// for the embedded server, whose port comes from the OS rather than the range. A
// wildcard bind alone does not avoid a port another process holds on a loopback
// address, as reserve explains, so a port that is not clear is passed over and
// another drawn, up to osPortTries times. The port is not tracked and nothing
// holds it once chooseOSPort returns.
func (a *portAllocator) chooseOSPort(draw func() (int, error)) (int, error) {
	for range osPortTries {
		port, err := draw()
		if err != nil {
			return 0, fmt.Errorf("could not draw a port from the OS: %w", err)
		}

		if a.loopbackFree(port) {
			return port, nil
		}
	}

	return 0, fmt.Errorf("%w: tried %d ports", ErrEmbeddedPortExhausted, osPortTries)
}

// drawOSPort has the OS pick a port by binding the wildcard address on port 0,
// and closes the listener. Go's tcp network makes this the dual-stack bind
// nats-server makes for its client port.
func drawOSPort() (int, error) {
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4zero})
	if err != nil {
		return 0, err
	}
	defer ln.Close()

	return ln.Addr().(*net.TCPAddr).Port, nil
}

// release returns ports to the range.
func (a *portAllocator) release(ports ...int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, port := range ports {
		delete(a.inUse, port)
	}
}
