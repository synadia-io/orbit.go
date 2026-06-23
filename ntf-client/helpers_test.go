package ntf

import (
	"net/url"
	"os"
)

// testServerURL returns the management URL of the ntf-server service the
// suite runs against. CI points this at the service container via the
// NATS_TESTER_URL environment variable; locally it defaults to a service
// listening on the loopback.
func testServerURL() string {
	if u := os.Getenv("NATS_TESTER_URL"); u != "" {
		return u
	}

	return "nats://localhost:4222"
}

// testServerHost returns just the host portion of testServerURL. Tests use it
// to build listener URLs (websocket, ...) and TLS SANs that must match the host
// the suite dials — which is the service hostname (e.g. "nats") in CI and
// "localhost" locally. Defaults to "localhost".
func testServerHost() string {
	u, err := url.Parse(testServerURL())
	if err != nil || u.Hostname() == "" {
		return "localhost"
	}

	return u.Hostname()
}
