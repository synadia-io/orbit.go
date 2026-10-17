package ntf

import (
	"context"
	"maps"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestWithToxicsOption pins that WithToxics sets the toxics option, which is
// off without it.
func TestWithToxicsOption(t *testing.T) {
	co := resolveCreateOptions([]CreateOption{WithToxics()})
	if !co.toxics {
		t.Fatal("WithToxics did not set toxics")
	}

	plain := resolveCreateOptions(nil)
	if plain.toxics {
		t.Fatal("toxics set without WithToxics")
	}
}

// TestWithToxicsCreatesThroughProxies pins that every kind of create sends
// toxics and that ntf-server's network agent serves them: the instance lists
// its route and gateway links and a client link for each server, each server's
// port and url are its client proxy's, and a connection to the url reaches
// that server.
func TestWithToxicsCreatesThroughProxies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer m.Close()

	tests := []struct {
		name   string
		create func() (*Instance, error)
		kinds  map[string]int
	}{
		{"server", func() (*Instance, error) {
			return m.CreateServer(ctx, false, WithToxics(), WithDescription(t.Name()))
		}, map[string]int{"client": 1}},
		{"cluster", func() (*Instance, error) {
			return m.CreateCluster(ctx, 2, false, WithToxics(), WithDescription(t.Name()))
		}, map[string]int{"route": 2, "client": 2}},
		{"super-cluster", func() (*Instance, error) {
			return m.CreateSuperCluster(ctx, 2, 2, false, WithToxics(), WithDescription(t.Name()))
		}, map[string]int{"route": 4, "gateway": 4, "client": 4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst, err := tt.create()
			if err != nil {
				t.Fatalf("create with toxics: %v", err)
			}
			defer inst.Destroy(t)

			kinds := map[string]int{}
			proxies := map[string]string{}
			for _, lk := range inst.Links {
				kinds[lk.Kind]++
				if lk.Kind == "client" {
					proxies[lk.To] = lk.Proxy
				}
			}
			if !maps.Equal(kinds, tt.kinds) {
				t.Errorf("links by kind = %v, want %v", kinds, tt.kinds)
			}

			for _, srv := range inst.Servers {
				port := ":" + strconv.Itoa(srv.Port)
				if !strings.HasSuffix(proxies[srv.Name], port) || !strings.HasSuffix(srv.URL, port) {
					t.Errorf("server %s port %d and url %s are not its client proxy %s", srv.Name, srv.Port, srv.URL, proxies[srv.Name])
				}

				nc, err := nats.Connect(srv.URL, nats.Timeout(5*time.Second))
				if err != nil {
					t.Errorf("connect to %s at %s: %v", srv.Name, srv.URL, err)
					continue
				}
				if nc.ConnectedServerName() != srv.Name {
					t.Errorf("url of %s reached %s", srv.Name, nc.ConnectedServerName())
				}
				nc.Close()
			}
		})
	}
}
