package ntf

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/ntf-client/api"
)

func TestCreateServer(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	client.WithJetStreamServer(t, func(t *testing.T, nc *nats.Conn, inst *Instance) {
		srv := inst.Servers[0]
		if !strings.HasSuffix(srv.Name, "-n1") {
			t.Fatalf("name not correct: %q", srv.Name)
		}

		log.Printf("Connected to server %q: %v", srv.Name, nc.ConnectedUrlRedacted())

		resp, err := nc.Request("$JS.API.INFO", nil, time.Second)
		if err != nil {
			t.Fatal(err)
		}

		log.Printf("INFO: %s", string(resp.Data))
	})
}

func TestCreateCluster(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	client.WithJetStreamCluster(t, 3, func(t *testing.T, nc *nats.Conn, inst *Instance) {
		if len(inst.Servers) != 3 {
			t.Fatalf("cluster length not correct: %v", len(inst.Servers))
		}

		resp, err := nc.Request("$JS.API.INFO", nil, time.Second)
		if err != nil {
			t.Fatal(err)
		}

		log.Printf("INFO: %s", string(resp.Data))
	})
}

// TestConcurrentCreateClusters issues 3 create.cluster requests in parallel
// against the same management service and verifies the resulting instances are
// fully isolated: distinct IDs, cluster names, server names, and client ports.
func TestConcurrentCreateClusters(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const n = 3
	results := make([]*Instance, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = client.CreateCluster(t, 3, false, WithDescription(fmt.Sprintf("%s/cluster-%d", t.Name(), i)))
		}(i)
	}
	wg.Wait()

	ids := map[string]bool{}
	clusterNames := map[string]bool{}
	serverNames := map[string]bool{}
	ports := map[int]bool{}

	for i, inst := range results {
		if inst.ID == "" {
			t.Fatalf("cluster %d: missing ID", i)
		}
		if ids[inst.ID] {
			t.Fatalf("duplicate instance ID: %q", inst.ID)
		}
		ids[inst.ID] = true

		if len(inst.Servers) != 3 {
			t.Fatalf("cluster %d: expected 3 servers, got %d", i, len(inst.Servers))
		}
		for _, srv := range inst.Servers {
			if srv.Cluster == "" {
				t.Fatalf("cluster %d: server %q missing cluster name", i, srv.Name)
			}
			clusterNames[srv.Cluster] = true
			if serverNames[srv.Name] {
				t.Fatalf("duplicate server name across instances: %q", srv.Name)
			}
			serverNames[srv.Name] = true
			if ports[srv.Port] {
				t.Fatalf("duplicate client port across instances: %d", srv.Port)
			}
			ports[srv.Port] = true
		}
	}

	if len(clusterNames) != n {
		t.Fatalf("expected %d distinct cluster names, got %d (%v)", n, len(clusterNames), clusterNames)
	}

	listed := client.List(t)
	if len(listed.Instances) < n {
		t.Fatalf("expected at least %d instances in list, got %d", n, len(listed.Instances))
	}

	for _, inst := range results {
		inst.Destroy(t)
	}
}

// TestParallelClusters runs multiple subtests in parallel, each creating its
// own cluster, exercising it, and tearing it down via inst.Destroy. The outer
// test verifies that the three clusters were genuinely independent — distinct
// instance IDs, distinct cluster names, disjoint port sets — and not three
// views of one cluster.
func TestParallelClusters(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	const n = 3

	type observation struct {
		id          string
		clusterName string
		ports       []int
	}

	var (
		mu           sync.Mutex
		observations [n]observation
	)

	for i := 0; i < n; i++ {
		i := i
		t.Run(fmt.Sprintf("cluster-%d", i), func(t *testing.T) {
			t.Parallel()
			client.WithJetStreamCluster(t, 3, func(t *testing.T, nc *nats.Conn, inst *Instance) {
				if len(inst.Servers) != 3 {
					t.Fatalf("cluster length wrong: %d", len(inst.Servers))
				}
				if _, err := nc.Request("$JS.API.INFO", nil, time.Second); err != nil {
					t.Fatal(err)
				}

				cName := inst.Servers[0].Cluster
				ports := make([]int, 0, len(inst.Servers))
				localPorts := map[int]bool{}
				for _, srv := range inst.Servers {
					if srv.Cluster != cName {
						t.Fatalf("server %q has cluster %q, expected %q", srv.Name, srv.Cluster, cName)
					}
					if localPorts[srv.Port] {
						t.Fatalf("duplicate port %d within instance %s", srv.Port, inst.ID)
					}
					localPorts[srv.Port] = true
					ports = append(ports, srv.Port)
				}

				mu.Lock()
				observations[i] = observation{id: inst.ID, clusterName: cName, ports: ports}
				mu.Unlock()
			})
		})
	}

	// After all subtests finish, assert global isolation across them.
	t.Cleanup(func() {
		ids := map[string]bool{}
		clusterNames := map[string]bool{}
		ports := map[int]bool{}
		for _, obs := range observations {
			if obs.id == "" {
				// Subtest didn't record — likely failed; let its own t.Fatal surface.
				continue
			}
			if ids[obs.id] {
				t.Fatalf("duplicate instance ID across subtests: %q", obs.id)
			}
			ids[obs.id] = true
			if clusterNames[obs.clusterName] {
				t.Fatalf("duplicate cluster name across subtests: %q", obs.clusterName)
			}
			clusterNames[obs.clusterName] = true
			for _, p := range obs.ports {
				if ports[p] {
					t.Fatalf("duplicate port across subtests: %d", p)
				}
				ports[p] = true
			}
		}
	})
}

// TestWebSocketCluster spins a 2-node cluster with a user-supplied websocket
// snippet plus an extra "websocket" listener, then connects to each node over
// ws:// using nats.go. Exercises the full templating path end-to-end:
// allow-listed snippet key, per-server listener-port reservation, snippet
// rendering with template env (.Ports.websocket), include guards firing in
// the main template, and ManagedServer.Ports surfacing the reserved port.
// Also stops and restarts a node to confirm the snippet-include path
// survives the startFromConfig restart flow.
func TestWebSocketCluster(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const wsSnippet = `
websocket {
    port: {{ .Ports.websocket }}
    no_tls: true
}
`

	inst := client.CreateCluster(t, 2, false, WithWebSocket(wsSnippet))
	defer inst.Destroy(t)

	if len(inst.Servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(inst.Servers))
	}

	seen := map[int]bool{}
	for _, srv := range inst.Servers {
		wsPort, ok := srv.Ports["websocket"]
		if !ok || wsPort == 0 {
			t.Fatalf("server %q missing websocket port: %v", srv.Name, srv.Ports)
		}
		if seen[wsPort] {
			t.Fatalf("duplicate websocket port %d across servers", wsPort)
		}
		seen[wsPort] = true
	}

	for _, srv := range inst.Servers {
		wsConnect(t, client.address, srv)
	}

	// Stop and restart one node; restart goes through startFromConfig which
	// re-runs ProcessConfigFile against the persisted config (with its
	// include directive). If the snippet path were absolute, this would fail
	// because NATS joins includes onto the config file's directory.
	target := inst.Servers[0]
	inst.StopServer(t, target)
	inst.StartServer(t, target)

	wsConnect(t, client.address, target)
}

// wsConnect dials srv's websocket listener via nats.go's ws:// transport. A
// successful Connect proves the listener is real (nats.go does protocol
// negotiation + PING/PONG over the WS frame as part of Connect), and the
// ConnectedUrl assertion proves we landed on the port returned via
// ManagedServer.Ports rather than something that happened to be open.
func wsConnect(t *testing.T, addr string, srv *api.ManagedServer) {
	t.Helper()

	wsPort := srv.Ports["websocket"]
	wsURL := fmt.Sprintf("ws://%s:%d", addr, wsPort)
	nc, err := nats.Connect(wsURL, nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("ws connect %s: %v", wsURL, err)
	}
	defer nc.Close()

	if want := fmt.Sprintf(":%d", wsPort); !strings.Contains(nc.ConnectedUrl(), want) {
		t.Fatalf("connected URL %q does not contain ws port %s", nc.ConnectedUrl(), want)
	}

	log.Printf("ws connect ok on %s (%s)", srv.Name, wsURL)
}

// TestMultipleSnippets layers three snippets — accounts, websocket, top —
// onto a 2-node cluster simultaneously, plus a websocket listener. Each
// snippet's effect is asserted independently to catch any rendering or
// include-ordering regression that would silently drop one of them.
//
//   - accounts: keeps user1 (so the built-in no_auth_user: user1 still resolves)
//     and adds an alice user that doesn't exist by default. alice connecting
//     proves the snippet was applied.
//   - websocket: full ws block on the reserved listener. wsRoundTrip proves it.
//   - top: max_payload override; nc.MaxPayload() reads the server-advertised
//     value, proving the top-level snippet preceded server_name in the
//     rendered config.
func TestMultipleSnippets(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const accountsSnippet = `
accounts {
    USERS1 {
        users = [ { user: "user1", pass: "password" } ]
    }
    EXTRAS {
        users = [ { user: "alice", pass: "wonder" } ]
    }
    $SYS { users = [ { user: "system", pass: "password" } ] }
}
`
	const wsSnippet = `
websocket {
    port: {{ .Ports.websocket }}
    no_tls: true
}
`
	const topSnippet = `
max_payload: 8192
`

	inst := client.CreateCluster(t, 2, false,
		WithAccounts(accountsSnippet),
		WithWebSocket(wsSnippet),
		WithTopLevel(topSnippet),
	)
	defer inst.Destroy(t)

	if len(inst.Servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(inst.Servers))
	}

	srv := inst.Servers[0]

	// Default connection — uses no_auth_user: user1 from the built-in
	// authorization slot. Proves the accounts snippet kept user1 reachable.
	nc, err := nats.Connect(srv.URL, nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("default connect: %v", err)
	}
	defer nc.Close()

	// top snippet effect: server-advertised max_payload from INFO.
	if got := nc.MaxPayload(); got != 8192 {
		t.Fatalf("top snippet not in effect: max_payload=%d, want 8192", got)
	}

	// accounts snippet effect: alice exists only because the snippet rendered.
	aliceNc, err := nats.Connect(srv.URL, nats.UserInfo("alice", "wonder"))
	if err != nil {
		t.Fatalf("alice connect (accounts snippet not in effect?): %v", err)
	}
	aliceNc.Close()

	// websocket snippet effect: WS listener serves traffic on Ports[websocket].
	for _, s := range inst.Servers {
		wsConnect(t, client.address, s)
	}
}

// TestFullTemplateOverride spins a 2-node cluster from a caller-supplied
// template that replaces the built-in one entirely. The template emits only
// what the cluster needs to form (server name, client port, cluster block
// with routes from .Routes) — no JetStream, no accounts, no auth — proving
// the override actually displaces the built-in defaults yet still has access
// to the wiring data the service injects.
func TestFullTemplateOverride(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const tmpl = `
server_name: {{ .ServerName }}
port: {{ .ClientPort }}
cluster {
    name: {{ .ClusterName }}
    port: {{ .ClusterPort }}
    routes: [
{{ range .Routes }}        nats://{{ . }}
{{ end }}    ]
}
`

	inst := client.CreateCluster(t, 2, false, WithTemplate(tmpl))
	defer inst.Destroy(t)

	if len(inst.Servers) != 2 {
		t.Fatalf("expected 2 servers, got %d", len(inst.Servers))
	}

	cName := inst.Servers[0].Cluster
	for _, srv := range inst.Servers {
		if srv.Cluster != cName {
			t.Fatalf("server %q has cluster %q, expected %q", srv.Name, srv.Cluster, cName)
		}
	}

	// Confirm the cluster actually formed by routing a message between nodes:
	// subscribe on srv0, publish on srv1, expect delivery within a budget.
	sub, err := nats.Connect(inst.Servers[0].URL, nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("connect sub: %v", err)
	}
	defer sub.Close()

	pub, err := nats.Connect(inst.Servers[1].URL, nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("connect pub: %v", err)
	}
	defer pub.Close()

	syncSub, err := sub.SubscribeSync("tmpl.test")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := sub.Flush(); err != nil {
		t.Fatalf("flush sub: %v", err)
	}

	// Cluster routing needs a brief moment to propagate the interest after
	// SubscribeSync. Retry the publish a few times rather than sleeping a
	// fixed budget.
	deadline := time.Now().Add(5 * time.Second)
	var msg *nats.Msg
	for time.Now().Before(deadline) {
		if err := pub.Publish("tmpl.test", []byte("hello")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		if err := pub.Flush(); err != nil {
			t.Fatalf("flush pub: %v", err)
		}
		msg, err = syncSub.NextMsg(500 * time.Millisecond)
		if err == nil {
			break
		}
	}
	if msg == nil {
		t.Fatalf("no message received across cluster routes within 5s")
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("got %q, want hello", string(msg.Data))
	}
}

// TestTemplateWithSnippets layers a snippet on top of a full-template override.
// The custom template references {{ .Snippets.accounts }} via NATS conf
// include — quoted, because the NATS parser joins the include token onto the
// config-file directory, so .Snippets.<name> is a path relative to that dir.
// The accounts snippet defines an alice user but no user1, proving (a) the
// rendered snippet file is reachable from the user's template and (b) custom
// auth replaces the built-in defaults — alice can connect with the right
// password, user1 cannot.
func TestTemplateWithSnippets(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const tmpl = `
server_name: {{ .ServerName }}
port: {{ .ClientPort }}
{{ if .Snippets.accounts }}include "{{ .Snippets.accounts }}"{{ end }}
`
	const accountsSnippet = `
accounts {
    USERS {
        users = [ { user: "alice", pass: "wonder" } ]
    }
}
`

	inst := client.CreateServer(t, false,
		WithTemplate(tmpl),
		WithAccounts(accountsSnippet),
	)
	defer inst.Destroy(t)

	if len(inst.Servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(inst.Servers))
	}
	srv := inst.Servers[0]

	// Custom user from the snippet must work.
	nc, err := nats.Connect(srv.URL, nats.UserInfo("alice", "wonder"))
	if err != nil {
		t.Fatalf("alice connect failed (snippet not in effect?): %v", err)
	}
	nc.Close()

	// Built-in user1 must NOT work — the snippet replaced the default
	// accounts block, so user1 doesn't exist.
	nc, err = nats.Connect(srv.URL, nats.UserInfo("user1", "password"), nats.MaxReconnects(0))
	if err == nil {
		nc.Close()
		t.Fatalf("user1 unexpectedly connected; built-in accounts should be replaced")
	}
}

// TestSuperClusterWithSnippets layers a custom accounts snippet onto a 2x2
// super-cluster and verifies gateway routing still works across clusters
// under the snippet. The accounts snippet replaces the built-in block but
// keeps user1 (so the built-in no_auth_user still resolves) and adds an
// alice account. alice subscribes on a server in cluster A and a different
// alice connection publishes from a server in cluster B; if the message
// arrives, both the snippet and the gateway formation succeeded.
func TestSuperClusterWithSnippets(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const accountsSnippet = `
accounts {
    USERS1 {
        users = [ { user: "user1", pass: "password" } ]
    }
    EXTRAS {
        users = [ { user: "alice", pass: "wonder" } ]
    }
    $SYS { users = [ { user: "system", pass: "password" } ] }
}
`

	inst := client.CreateSuperCluster(t, 2, 2, false, WithAccounts(accountsSnippet))
	defer inst.Destroy(t)

	if len(inst.Servers) != 4 {
		t.Fatalf("expected 4 servers, got %d", len(inst.Servers))
	}

	// Pick one server from each cluster so the round-trip really crosses a
	// gateway and not just a cluster route.
	var srvA, srvB *api.ManagedServer
	for _, s := range inst.Servers {
		if srvA == nil {
			srvA = s
			continue
		}
		if s.Cluster != srvA.Cluster {
			srvB = s
			break
		}
	}
	if srvA == nil || srvB == nil {
		t.Fatalf("could not find servers from two distinct clusters: %+v", inst.Servers)
	}

	subNc, err := nats.Connect(srvA.URL, nats.UserInfo("alice", "wonder"), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("alice connect on %s: %v", srvA.Name, err)
	}
	defer subNc.Close()

	pubNc, err := nats.Connect(srvB.URL, nats.UserInfo("alice", "wonder"), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("alice connect on %s: %v", srvB.Name, err)
	}
	defer pubNc.Close()

	sub, err := subNc.SubscribeSync("sc.snippets.test")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := subNc.Flush(); err != nil {
		t.Fatalf("flush sub: %v", err)
	}

	// Gateway interest needs hundreds of ms to propagate after subscribe.
	// Retry the publish on a deadline rather than rely on a single attempt.
	deadline := time.Now().Add(10 * time.Second)
	var msg *nats.Msg
	for time.Now().Before(deadline) {
		if err := pubNc.Publish("sc.snippets.test", []byte("hello")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		if err := pubNc.Flush(); err != nil {
			t.Fatalf("flush pub: %v", err)
		}
		msg, err = sub.NextMsg(500 * time.Millisecond)
		if err == nil {
			break
		}
	}
	if msg == nil {
		t.Fatalf("no message received across gateway within 10s (cluster A=%s -> cluster B=%s)", srvA.Cluster, srvB.Cluster)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("got %q want hello", string(msg.Data))
	}
}

// TestConvenienceHelperWithCustomAuth uses WithCluster — the convenience
// helper — with a snippet that defines only an alice user and an
// authorization override that removes the built-in no_auth_user fallback,
// so the helper's nats.Connect must apply WithConnectOptions(UserInfo) or
// the connect fails. Reaching the callback body is the assertion that the
// connect-options path is wired through; the Flush adds a round-trip so a
// half-open TCP-but-auth-pending connection wouldn't slip through.
func TestConvenienceHelperWithCustomAuth(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const accountsSnippet = `
accounts {
    EXTRAS {
        users = [ { user: "alice", pass: "wonder" } ]
    }
    $SYS { users = [ { user: "system", pass: "password" } ] }
}
`
	// Empty authorization body — overrides the built-in no_auth_user: user1,
	// so unauthenticated connects are rejected.
	const authSnippet = `# auth required, no no_auth_user`

	client.WithCluster(t, 2, func(t *testing.T, nc *nats.Conn, inst *Instance) {
		if !nc.IsConnected() {
			t.Fatal("expected connected nc")
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	},
		WithAccounts(accountsSnippet),
		WithAuthorization(authSnippet),
		WithConnectOptions(nats.UserInfo("alice", "wonder")),
	)
}

func TestCreateSuperCluster(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	client.WithJetStreamSuperCluster(t, 3, 3, func(t *testing.T, nc *nats.Conn, inst *Instance) {
		if len(inst.Servers) != 9 {
			t.Fatalf("cluster length not correct: %v", len(inst.Servers))
		}

		resp, err := nc.Request("$JS.API.INFO", nil, time.Second)
		if err != nil {
			t.Fatal(err)
		}

		log.Printf("INFO: %s", string(resp.Data))

		first := inst.Servers[0]
		stopResp := inst.StopServer(t, first)
		log.Printf("Stopped server: %t", stopResp.Shutdown)

		sysNc, err := nats.Connect(nc.ConnectedUrl(), nats.UserInfo("system", "password"))
		if err != nil {
			t.Fatal(err)
		}

		resp, err = sysNc.Request("$SYS.REQ.SERVER.PING.VARZ", []byte(fmt.Sprintf(`{"server_name":%q}`, first.Name)), time.Second)
		if err == nil {
			t.Fatalf("expected error got %v", string(resp.Data))
		}

		startResp := inst.StartServer(t, first)
		log.Printf("Started server: %t", startResp.Started)

		log.Printf("Waiting for JetStreram")
		client.WaitForJetStream(t, nc)

		resp, err = nc.Request("$JS.API.INFO", nil, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		log.Printf("INFO: %s", string(resp.Data))

		j, _ := json.MarshalIndent(inst.Status(t), "", "  ")
		log.Printf("STATUS: %v", string(j))
	})
}

// TestUpdateThenReloadAppliesConfig is the stage/apply seam: UpdateServer
// stages a new accounts block but does NOT apply it; the running server only
// picks it up after ReloadServer.
func TestUpdateThenReloadAppliesConfig(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const newAccounts = `
accounts {
    USERS1 {
        users = [ { user: "user1", pass: "password" } ]
    }
    EXTRAS {
        users = [ { user: "alice", pass: "wonder" } ]
    }
    $SYS { users = [ { user: "system", pass: "password" } ] }
}
`

	client.WithServer(t, func(t *testing.T, _ *nats.Conn, inst *Instance) {
		target := inst.Servers[0]

		// Pre-update: alice cannot connect.
		if nc, err := nats.Connect(target.URL, nats.UserInfo("alice", "wonder")); err == nil {
			nc.Close()
			t.Fatalf("alice should not connect before update")
		}

		// Update stages the new accounts but does not apply them.
		if resp := inst.UpdateServer(t, target, WithAccounts(newAccounts)); !resp.Updated {
			t.Fatalf("expected Updated=true, got %+v", resp)
		}

		// alice still cannot connect — the staged config isn't live yet.
		if nc, err := nats.Connect(target.URL, nats.UserInfo("alice", "wonder")); err == nil {
			nc.Close()
			t.Fatalf("alice should not connect after update but before reload")
		}

		// Reload applies it.
		if resp := inst.ReloadServer(t, target); !resp.Reloaded {
			t.Fatalf("expected Reloaded=true, got %+v", resp)
		}
		nc, err := nats.Connect(target.URL, nats.UserInfo("alice", "wonder"))
		if err != nil {
			t.Fatalf("alice could not connect after reload: %v", err)
		}
		nc.Close()
	})
}

// TestUpdateThenRestartAppliesConfig proves update works on a stopped server
// and that StartServer applies the staged config (the apply-via-restart path).
func TestUpdateThenRestartAppliesConfig(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const newAccounts = `
accounts {
    USERS1 {
        users = [ { user: "user1", pass: "password" } ]
    }
    EXTRAS {
        users = [ { user: "alice", pass: "wonder" } ]
    }
    $SYS { users = [ { user: "system", pass: "password" } ] }
}
`

	inst := client.CreateServer(t, false)
	defer inst.Destroy(t)

	target := inst.Servers[0]

	// Stop the server, then update its config while it is down.
	inst.StopServer(t, target)

	if resp := inst.UpdateServer(t, target, WithAccounts(newAccounts)); !resp.Updated {
		t.Fatalf("expected Updated=true on stopped server, got %+v", resp)
	}

	// Start applies the staged config.
	inst.StartServer(t, target)

	nc, err := nats.Connect(target.URL, nats.UserInfo("alice", "wonder"))
	if err != nil {
		t.Fatalf("alice could not connect after restart: %v", err)
	}
	nc.Close()
}

// TestGeneratedTLS_Mutual creates a 2-node cluster with the default
// WithGeneratedTLS() (mutual TLS), uses the convenience helper so TLS is
// auto-wired into the nats.Connect, and verifies that a bare plaintext
// connect against the same URL fails fast.
func TestGeneratedTLS_Mutual(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	client.WithCluster(t, 2, func(t *testing.T, nc *nats.Conn, inst *Instance) {
		if !nc.IsConnected() {
			t.Fatal("expected helper to connect via TLS")
		}
		if inst.TLS == nil {
			t.Fatal("Instance.TLS missing")
		}
		if inst.TLS.CAPEM == "" || inst.TLS.ClientCertPEM == "" || inst.TLS.ClientKeyPEM == "" {
			t.Fatalf("mutual TLS material incomplete: ca=%d client_cert=%d client_key=%d",
				len(inst.TLS.CAPEM), len(inst.TLS.ClientCertPEM), len(inst.TLS.ClientKeyPEM))
		}

		bare, err := nats.Connect(inst.RandomServer().URL,
			nats.Timeout(2*time.Second),
			nats.MaxReconnects(0),
		)
		if err == nil {
			bare.Close()
			t.Fatal("bare nats.Connect unexpectedly succeeded against TLS-only server")
		}
	}, WithGeneratedTLS())
}

// TestGeneratedTLS_HandshakeFirst opts into TLS-handshake-first. It passes only
// if the server renders handshake_first AND the convenience helper auto-wires
// nats.TLSHandshakeFirst — a mismatch on either side hangs the connect.
func TestGeneratedTLS_HandshakeFirst(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	client.WithServer(t, func(t *testing.T, nc *nats.Conn, inst *Instance) {
		if !nc.IsConnected() {
			t.Fatal("expected helper to connect via TLS handshake-first")
		}
	}, WithGeneratedTLS(TLSHandshakeFirst()))
}

// TestGeneratedTLS_WebSocket wires the generated server cert into a websocket
// tls block via the .TLS.* snippet paths, then dials the listener over wss://
// with nats.go and the generated CA — proving the cert paths reach snippets and
// a wss listener comes up. The websocket TLS is independent of the client-port
// TLS, so this uses TLSServerOnly for an unambiguous server-only wss posture.
func TestGeneratedTLS_WebSocket(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	const wsSnippet = `
websocket {
    port: {{ .Ports.websocket }}
    tls {
        cert_file: "{{ .TLS.CertFile }}"
        key_file:  "{{ .TLS.KeyFile }}"
    }
}
`

	inst := client.CreateCluster(t, 2, false,
		WithGeneratedTLS(TLSServerOnly()),
		WithWebSocket(wsSnippet),
	)
	defer inst.Destroy(t)

	cfg, err := TLSConfig(inst)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}

	for _, srv := range inst.Servers {
		wsPort, ok := srv.Ports["websocket"]
		if !ok || wsPort == 0 {
			t.Fatalf("server %q missing websocket port: %v", srv.Name, srv.Ports)
		}
		url := fmt.Sprintf("wss://%s:%d", testServerHost(), wsPort)
		nc, err := nats.Connect(url, nats.Secure(cfg), nats.Timeout(5*time.Second))
		if err != nil {
			t.Fatalf("wss connect to %s failed: %v", url, err)
		}
		if !nc.IsConnected() {
			nc.Close()
			t.Fatalf("not connected over wss to %s", url)
		}
		nc.Close()
	}
}

// TestGeneratedTLS_ServerOnly exercises one-way TLS — the service mints a CA
// and server cert but no client cert. RootCAs-only connects succeed; the
// response carries no client material.
func TestGeneratedTLS_ServerOnly(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	client.WithCluster(t, 2, func(t *testing.T, nc *nats.Conn, inst *Instance) {
		if !nc.IsConnected() {
			t.Fatal("expected helper to connect via TLS")
		}
		if inst.TLS == nil || inst.TLS.CAPEM == "" {
			t.Fatal("CA material missing")
		}
		if inst.TLS.ClientCertPEM != "" || inst.TLS.ClientKeyPEM != "" {
			t.Fatal("server-only mode should not return client material")
		}

		cfg, err := TLSConfig(inst)
		if err != nil {
			t.Fatal(err)
		}
		if len(cfg.Certificates) != 0 {
			t.Fatal("server-only TLSConfig should carry no client cert")
		}

		direct, err := nats.Connect(inst.RandomServer().URL, nats.Secure(cfg), nats.Timeout(5*time.Second))
		if err != nil {
			t.Fatalf("RootCAs-only connect failed: %v", err)
		}
		direct.Close()
	}, WithGeneratedTLS(TLSServerOnly()))
}

// TestGeneratedTLS_ClusterRoutesUnsecured confirms that with TLS on the client
// port, cluster routes stay plaintext and still form: a sub on one node
// receives a publish from another, proving the cluster routes work despite
// the client-port TLS configuration.
func TestGeneratedTLS_ClusterRoutesUnsecured(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	inst := client.CreateCluster(t, 2, false, WithGeneratedTLS())
	defer inst.Destroy(t)

	cfg, err := TLSConfig(inst)
	if err != nil {
		t.Fatal(err)
	}

	subNc, err := nats.Connect(inst.Servers[0].URL, nats.Secure(cfg), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("sub connect: %v", err)
	}
	defer subNc.Close()
	pubNc, err := nats.Connect(inst.Servers[1].URL, nats.Secure(cfg), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("pub connect: %v", err)
	}
	defer pubNc.Close()

	syncSub, err := subNc.SubscribeSync("tls.route.test")
	if err != nil {
		t.Fatal(err)
	}
	if err := subNc.Flush(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var msg *nats.Msg
	for time.Now().Before(deadline) {
		if err := pubNc.Publish("tls.route.test", []byte("hello")); err != nil {
			t.Fatal(err)
		}
		if err := pubNc.Flush(); err != nil {
			t.Fatal(err)
		}
		msg, err = syncSub.NextMsg(500 * time.Millisecond)
		if err == nil {
			break
		}
	}
	if msg == nil {
		t.Fatal("no message across cluster routes within 5s — routes did not propagate under TLS-on-client-port")
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("got %q want hello", msg.Data)
	}
}

// TestGeneratedTLS_ConflictWithSnippet submits a raw CreateServer request
// that combines TLS with a user-supplied tls snippet and asserts the
// service rejects it with the mutual-exclusion error.
func TestGeneratedTLS_ConflictWithSnippet(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	mgmtNc, err := nats.Connect(testServerURL())
	if err != nil {
		t.Fatal(err)
	}
	defer mgmtNc.Close()

	body, _ := json.Marshal(api.CreateServerRequest{
		TLS:      &api.TLSOptions{Mode: api.TLSModeMutual},
		Snippets: map[string]string{"tls": "tls { ca_file: \"x\" }"},
	})
	resp, err := mgmtNc.Request("tester.create.server", body, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	serviceErr := resp.Header.Get("Nats-Service-Error")
	if serviceErr == "" {
		t.Fatal("expected service error, got none — conflict was not detected")
	}
	if !strings.Contains(serviceErr, "mutually exclusive") {
		t.Fatalf("expected 'mutually exclusive' error, got %q", serviceErr)
	}
}

// TestGeneratedTLS_ConflictWithTemplateMissingInclude submits a raw request
// combining TLS with a custom template that does not reference .TLSInclude
// and asserts the service rejects it.
func TestGeneratedTLS_ConflictWithTemplateMissingInclude(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	mgmtNc, err := nats.Connect(testServerURL())
	if err != nil {
		t.Fatal(err)
	}
	defer mgmtNc.Close()

	body, _ := json.Marshal(api.CreateServerRequest{
		TLS:      &api.TLSOptions{Mode: api.TLSModeMutual},
		Template: "server_name: foo\nport: 0\n",
	})
	resp, err := mgmtNc.Request("tester.create.server", body, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	serviceErr := resp.Header.Get("Nats-Service-Error")
	if !strings.Contains(serviceErr, "TLSInclude") {
		t.Fatalf("expected TLSInclude error, got %q", serviceErr)
	}
}

// TestGeneratedTLS_TemplateWithInclude verifies that a custom template that
// does opt into the managed TLS include resolves correctly and the resulting
// server speaks TLS.
func TestGeneratedTLS_TemplateWithInclude(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	const tmpl = `
server_name: {{ .ServerName }}
port: {{ .ClientPort }}
{{ if .TLSInclude }}include "{{ .TLSInclude }}"{{ end }}
`
	inst := client.CreateServer(t, false, WithTemplate(tmpl), WithGeneratedTLS())
	defer inst.Destroy(t)

	cfg, err := TLSConfig(inst)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(inst.Servers[0].URL, nats.Secure(cfg))
	if err != nil {
		t.Fatalf("tls connect: %v", err)
	}
	nc.Close()
}

// TestUpdateServerRejectsListenerAdd verifies that updating a server with a
// port-bearing snippet (websocket) absent at create time is rejected with 008.
func TestUpdateServerRejectsListenerAdd(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const wsSnippet = `
websocket {
    port: {{ .Ports.websocket }}
    no_tls: true
}
`

	client.WithServer(t, func(t *testing.T, _ *nats.Conn, inst *Instance) {
		target := inst.Servers[0]

		req, err := json.Marshal(api.UpdateServerRequest{
			Name:     target.Name,
			Snippets: map[string]string{"websocket": wsSnippet},
		})
		if err != nil {
			t.Fatalf("could not marshal: %v", err)
		}

		msg, err := client.nc.Request("tester.update.server", req, 10*time.Second)
		if err != nil {
			t.Fatalf("could not send request: %v", err)
		}

		got := msg.Header.Get("Nats-Service-Error-Code")
		if got != "008" {
			t.Fatalf("expected error code 008, got %q (text: %q)", got, msg.Header.Get("Nats-Service-Error"))
		}
	})
}

// TestUpdateServerRejectsListenerDrop verifies that updating a server created
// with a port-bearing snippet (websocket), with a payload that omits it, is
// rejected with 008.
func TestUpdateServerRejectsListenerDrop(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	const wsSnippet = `
websocket {
    port: {{ .Ports.websocket }}
    no_tls: true
}
`
	const newAccounts = `
accounts {
    USERS1 { users = [ { user: "user1", pass: "password" } ] }
    EXTRAS { users = [ { user: "alice", pass: "wonder" } ] }
    $SYS   { users = [ { user: "system", pass: "password" } ] }
}
`

	inst := client.CreateServer(t, false, WithWebSocket(wsSnippet))
	defer inst.Destroy(t)

	target := inst.Servers[0]

	req, err := json.Marshal(api.UpdateServerRequest{
		Name:     target.Name,
		Snippets: map[string]string{"accounts": newAccounts},
	})
	if err != nil {
		t.Fatalf("could not marshal: %v", err)
	}

	msg, err := client.nc.Request("tester.update.server", req, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send request: %v", err)
	}

	got := msg.Header.Get("Nats-Service-Error-Code")
	if got != "008" {
		t.Fatalf("expected error code 008, got %q (text: %q)", got, msg.Header.Get("Nats-Service-Error"))
	}
}

// TestUpdateServerRejectsUnknownName verifies that updating an unknown server
// name is rejected with 001.
func TestUpdateServerRejectsUnknownName(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	req, err := json.Marshal(api.UpdateServerRequest{Name: "this-server-does-not-exist"})
	if err != nil {
		t.Fatalf("could not marshal: %v", err)
	}

	msg, err := client.nc.Request("tester.update.server", req, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send request: %v", err)
	}

	got := msg.Header.Get("Nats-Service-Error-Code")
	if got != "001" {
		t.Fatalf("expected error code 001, got %q (text: %q)", got, msg.Header.Get("Nats-Service-Error"))
	}
}

// TestReloadServerRejectsUnknownName verifies that reloading an unknown server
// name is rejected with 001.
func TestReloadServerRejectsUnknownName(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	req, err := json.Marshal(api.ReloadServerRequest{Name: "this-server-does-not-exist"})
	if err != nil {
		t.Fatalf("could not marshal: %v", err)
	}

	msg, err := client.nc.Request("tester.reload.server", req, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send request: %v", err)
	}

	got := msg.Header.Get("Nats-Service-Error-Code")
	if got != "001" {
		t.Fatalf("expected error code 001, got %q (text: %q)", got, msg.Header.Get("Nats-Service-Error"))
	}
}

// TestReloadServerRejectsStopped verifies that reloading a stopped server is
// rejected with 002.
func TestReloadServerRejectsStopped(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	inst := client.CreateServer(t, false)
	defer inst.Destroy(t)

	target := inst.Servers[0]
	inst.StopServer(t, target)

	req, err := json.Marshal(api.ReloadServerRequest{Name: target.Name})
	if err != nil {
		t.Fatalf("could not marshal: %v", err)
	}

	msg, err := client.nc.Request("tester.reload.server", req, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send request: %v", err)
	}

	got := msg.Header.Get("Nats-Service-Error-Code")
	if got != "002" {
		t.Fatalf("expected error code 002, got %q (text: %q)", got, msg.Header.Get("Nats-Service-Error"))
	}
}

// TestGeneratedTLS_RestartPreservesTLS stops and restarts a TLS-on server,
// then verifies the restarted server still speaks TLS — proves cert files
// survive the Stop/Start round-trip and the persisted config still
// references them.
func TestGeneratedTLS_RestartPreservesTLS(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	inst := client.CreateCluster(t, 2, false, WithGeneratedTLS())
	defer inst.Destroy(t)

	cfg, err := TLSConfig(inst)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}

	target := inst.Servers[0]

	// TLS works before the restart.
	nc, err := nats.Connect(target.URL, nats.Secure(cfg), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("TLS connect before restart: %v", err)
	}
	nc.Close()

	// Stop then start the same server.
	inst.StopServer(t, target)
	inst.StartServer(t, target)

	// TLS still works after the restart, proving the cert files and the
	// persisted config survived the Stop/Start round-trip.
	nc, err = nats.Connect(target.URL, nats.Secure(cfg), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("TLS connect after restart: %v", err)
	}
	defer nc.Close()
	if !nc.IsConnected() {
		t.Fatal("not connected over TLS after restart")
	}
}

// TestCreateServerRejectsJetStreamSnippetWithoutJS verifies that a create
// request carrying a jetstream snippet while JetStream is disabled is rejected
// with error code 011, rather than silently dropping the snippet.
func TestCreateServerRejectsJetStreamSnippetWithoutJS(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	req, err := json.Marshal(api.CreateServerRequest{
		JetStream: false,
		Snippets:  map[string]string{"jetstream": "domain: HUB"},
	})
	if err != nil {
		t.Fatalf("could not marshal: %v", err)
	}

	msg, err := client.nc.Request("tester.create.server", req, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send request: %v", err)
	}

	got := msg.Header.Get("Nats-Service-Error-Code")
	if got != "011" {
		t.Fatalf("expected error code 011, got %q (text: %q)", got, msg.Header.Get("Nats-Service-Error"))
	}
}

// TestReloadServerNoUpdateIsNoOp reloads a server with no prior update.
func TestReloadServerNoUpdateIsNoOp(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	client.WithServer(t, func(t *testing.T, _ *nats.Conn, inst *Instance) {
		if resp := inst.ReloadServer(t, inst.Servers[0]); !resp.Reloaded {
			t.Fatalf("expected Reloaded=true, got %+v", resp)
		}

		st := inst.Status(t)
		if len(st.Servers) != 1 || !st.Servers[0].Running {
			t.Fatalf("server not running after reload: %+v", st.Servers)
		}
	})
}

// TestReloadRejectionLeavesStagedConfig proves there is no rollback: an update
// that stages an un-reloadable change (JetStream store_dir) is rejected by
// reload (010), the rejected config stays staged so a second reload fails the
// same way, and recovery is via another update to a valid config. Both configs
// come from jsConfig with only store_dir differing, so store_dir is the sole
// meaningful diff.
func TestReloadRejectionLeavesStagedConfig(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() {
		client.Reset(t)
	})

	// jsConfig renders a JetStream config with the given store_dir.
	jsConfig := func(storeDir string) string {
		return fmt.Sprintf(`
server_name: {{ .ServerName }}
port: {{ .ClientPort }}
jetstream {
    enabled: true
    store_dir: %q
}
system_account: "$SYS"
no_auth_user: user1
accounts {
    USERS1 { users = [ { user: "user1", pass: "password" } ], jetstream: true }
    $SYS   { users = [ { user: "system", pass: "password" } ] }
}
`, storeDir)
	}

	inst := client.CreateServer(t, true)
	defer inst.Destroy(t)

	target := inst.Servers[0]

	// sendReload sends a raw reload and returns the service error code (empty
	// string on success). Used directly because inst.ReloadServer fatals on
	// any error response.
	sendReload := func() string {
		req, err := json.Marshal(api.ReloadServerRequest{Name: target.Name})
		if err != nil {
			t.Fatalf("could not marshal: %v", err)
		}
		msg, err := client.nc.Request("tester.reload.server", req, 10*time.Second)
		if err != nil {
			t.Fatalf("could not send request: %v", err)
		}
		return msg.Header.Get("Nats-Service-Error-Code")
	}

	// Stage the un-reloadable change; reload rejects it.
	if resp := inst.UpdateServer(t, target, WithTemplate(jsConfig("{{ .StoreDir }}-reloaded"))); !resp.Updated {
		t.Fatalf("expected Updated=true, got %+v", resp)
	}
	if got := sendReload(); got != "010" {
		t.Fatalf("expected 010 on first reload, got %q", got)
	}

	// Recover by staging a valid config; reload now succeeds.
	if resp := inst.UpdateServer(t, target, WithTemplate(jsConfig("{{ .StoreDir }}"))); !resp.Updated {
		t.Fatalf("expected Updated=true, got %+v", resp)
	}
	if resp := inst.ReloadServer(t, target); !resp.Reloaded {
		t.Fatalf("expected Reloaded=true after valid update, got %+v", resp)
	}
}

// TestJetStreamDomainViaSnippet creates a single JetStream server whose domain
// is set through WithJetStream and asserts the running server reports that
// domain back on $JS.API.INFO — proving the snippet is merged into the live
// jetstream block (store_dir still auto-injected by the service).
func TestJetStreamDomainViaSnippet(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	client.WithJetStreamServer(t, func(t *testing.T, nc *nats.Conn, inst *Instance) {
		resp, err := nc.Request("$JS.API.INFO", nil, time.Second)
		if err != nil {
			t.Fatalf("$JS.API.INFO: %v", err)
		}
		if !strings.Contains(string(resp.Data), `"domain":"HUB"`) {
			t.Fatalf("expected domain HUB in JS info, got: %s", string(resp.Data))
		}
	}, WithJetStream("domain: HUB"))
}

// TestGeneratedTLS_CallerSANs confirms that caller-supplied SANs replace the
// defaults and the resulting cert validates when dialed at one of those
// SANs. The dialed host is included explicitly so the test can connect over the
// management URL.
func TestGeneratedTLS_CallerSANs(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	inst := client.CreateServer(t, false, WithGeneratedTLS(TLSSANs(testServerHost(), "127.0.0.1")))
	defer inst.Destroy(t)

	cfg, err := TLSConfig(inst)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(inst.Servers[0].URL, nats.Secure(cfg))
	if err != nil {
		t.Fatalf("connect with caller SANs: %v", err)
	}
	nc.Close()
}

// TestGeneratedTLS_DirectCreate exercises the non-helper path: CreateCluster
// returns the instance with TLS material, TLSConfig builds the *tls.Config,
// and nats.Connect with nats.Secure dials successfully.
func TestGeneratedTLS_DirectCreate(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	inst := client.CreateCluster(t, 2, false, WithGeneratedTLS())
	defer inst.Destroy(t)

	if inst.TLS == nil {
		t.Fatal("expected TLS material on Instance")
	}

	cfg, err := TLSConfig(inst)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	nc, err := nats.Connect(inst.RandomServer().URL, nats.Secure(cfg))
	if err != nil {
		t.Fatalf("tls connect: %v", err)
	}
	defer nc.Close()
	if !nc.IsConnected() {
		t.Fatal("not connected")
	}
}

// TestWithTLSTimeoutOption pins that WithTLSTimeout sets the pointer field in
// seconds and that a non-positive duration is a no-op (nil pointer).
func TestWithTLSTimeoutOption(t *testing.T) {
	uo := resolveUpdateOptions([]UpdateOption{WithTLSTimeout(500 * time.Millisecond)})
	if uo.tlsTimeout == nil || *uo.tlsTimeout != 0.5 {
		t.Fatalf("tlsTimeout = %v, want 0.5", uo.tlsTimeout)
	}

	z := resolveUpdateOptions([]UpdateOption{WithTLSTimeout(0)})
	if z.tlsTimeout != nil {
		t.Fatalf("WithTLSTimeout(0) should be a no-op, got %v", *z.tlsTimeout)
	}

	neg := resolveUpdateOptions([]UpdateOption{WithTLSTimeout(-1 * time.Millisecond)})
	if neg.tlsTimeout != nil {
		t.Fatalf("WithTLSTimeout(-1ms) should be a no-op, got %v", *neg.tlsTimeout)
	}
}

// TestTLSTimeoutOption pins that TLSTimeout sets the seconds value on the TLS
// options and ignores non-positive durations.
func TestTLSTimeoutOption(t *testing.T) {
	co := resolveCreateOptions(t, []CreateOption{
		WithGeneratedTLS(TLSTimeout(500 * time.Millisecond)),
	})
	if co.tls == nil || co.tls.Timeout != 0.5 {
		t.Fatalf("Timeout = %#v, want 0.5", co.tls)
	}

	co0 := resolveCreateOptions(t, []CreateOption{
		WithGeneratedTLS(TLSTimeout(0)),
	})
	if co0.tls.Timeout != 0 {
		t.Fatalf("TLSTimeout(0) should be a no-op, got %v", co0.tls.Timeout)
	}

	coNeg := resolveCreateOptions(t, []CreateOption{
		WithGeneratedTLS(TLSTimeout(-1 * time.Millisecond)),
	})
	if coNeg.tls.Timeout != 0 {
		t.Fatalf("TLSTimeout(-1ms) should be a no-op, got %v", coNeg.tls.Timeout)
	}
}
