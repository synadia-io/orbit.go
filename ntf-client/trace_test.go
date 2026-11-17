package ntf

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestWithTraceCapture(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	inst := client.CreateServer(t, true, WithTraceCapture())
	defer inst.Destroy(t)

	srv := inst.Servers[0]
	if srv.TraceURL == "" {
		t.Fatal("expected a TraceURL on a trace-enabled server")
	}
	if srv.Ports["trace"] == 0 {
		t.Fatal("expected Ports[\"trace\"] to be set")
	}

	// A connection through the proxy is captured; its CONNECT name is recorded
	// as metadata (but is no longer used to decide whether to keep the capture).
	nc, err := nats.Connect(srv.TraceURL, nats.Name("tester/trace/keep"))
	if err != nil {
		t.Fatalf("connect via trace url: %v", err)
	}
	sub, err := nc.SubscribeSync("demo")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Publish("demo", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("next msg: %v", err)
	}
	nc.Close()

	store := inst.TraceStore(t)
	info := waitForTraceObject(t, store, inst.ID, 5*time.Second)

	if got := info.Metadata["client_name"]; got != "tester/trace/keep" {
		t.Errorf("metadata client_name = %q, want tester/trace/keep", got)
	}
	if got := info.Metadata["server_name"]; got != srv.Name {
		t.Errorf("metadata server_name = %q, want %q", got, srv.Name)
	}
	if got := info.Metadata["format"]; got != "expanded" {
		t.Errorf("metadata format = %q, want expanded", got)
	}
	if got := info.Metadata["lang"]; got != "go" {
		t.Errorf("metadata lang = %q, want go", got)
	}
	if info.Metadata["version"] == "" {
		t.Error("metadata version is empty, want the nats.go client version")
	}
	if !strings.HasSuffix(info.Name, ".expanded.json") {
		t.Errorf("object name = %q, want .expanded.json suffix", info.Name)
	}
	if !strings.Contains(info.Name, "-go-") {
		t.Errorf("object name = %q, want it to embed the client lang", info.Name)
	}

	// A connection with an unrelated name is captured too — there is no filter.
	before := countTraceObjects(t, store, inst.ID)
	nc2, err := nats.Connect(srv.TraceURL, nats.Name("plain-consumer"))
	if err != nil {
		t.Fatalf("connect second client: %v", err)
	}
	if err := nc2.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	nc2.Close()
	if !waitForTraceCount(t, store, inst.ID, before+1, 5*time.Second) {
		t.Errorf("second connection was not captured: object count stayed at %d", before)
	}
}

func TestWithTraceCapture_Cluster(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Reset(t) })

	inst := client.CreateCluster(t, 3, false, WithTraceCapture())
	defer inst.Destroy(t)

	// Every server is fronted by a capture proxy of its own, and the first
	// server's TraceURL is still its own trace port.
	urls := map[string]bool{}
	for _, srv := range inst.Servers {
		if srv.TraceURL == "" {
			t.Fatalf("server %q carries no trace URL", srv.Name)
		}
		if urls[srv.TraceURL] {
			t.Fatalf("server %q shares trace URL %s", srv.Name, srv.TraceURL)
		}
		urls[srv.TraceURL] = true
	}
	first := inst.Servers[0]
	if !strings.HasSuffix(first.TraceURL, fmt.Sprintf(":%d", first.Ports["trace"])) {
		t.Errorf("first server's trace URL %s is not its trace port %d", first.TraceURL, first.Ports["trace"])
	}

	for _, srv := range inst.Servers {
		nc, err := nats.Connect(srv.TraceURL, nats.Name("tester/trace/cluster/"+srv.Name))
		if err != nil {
			t.Fatalf("connect via %s's trace url: %v", srv.Name, err)
		}
		sub, err := nc.SubscribeSync("demo")
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		err = nc.Publish("demo", []byte("hi"))
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
		err = nc.Flush()
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
		_, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("next msg through %s: %v", srv.Name, err)
		}
		nc.Close()
	}

	store := inst.TraceStore(t)
	if !waitForTraceCount(t, store, inst.ID, len(inst.Servers), 5*time.Second) {
		t.Fatalf("want a capture through each of the %d servers", len(inst.Servers))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	infos, err := store.List(ctx)
	if err != nil {
		t.Fatalf("list captures: %v", err)
	}
	servers := map[string]string{}
	for _, info := range infos {
		if info.Metadata["instance_id"] == inst.ID {
			servers[info.Metadata["client_name"]] = info.Metadata["server_name"]
		}
	}
	for _, srv := range inst.Servers {
		got := servers["tester/trace/cluster/"+srv.Name]
		if got != srv.Name {
			t.Errorf("capture through %s has server_name %q", srv.Name, got)
		}
	}
}

func waitForTraceObject(t testing.TB, store jetstream.ObjectStore, instanceID string, timeout time.Duration) *jetstream.ObjectInfo {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		infos, err := store.List(ctx)
		cancel()
		if err == nil {
			for _, info := range infos {
				if info.Metadata["instance_id"] == instanceID {
					return info
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no trace object for instance %s within %s (last list err: %v)", instanceID, timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitForTraceCount polls until at least want objects exist for the instance, or
// the timeout elapses. Captures upload asynchronously after a connection closes, so
// a positive assertion has to wait rather than check once.
func waitForTraceCount(t testing.TB, store jetstream.ObjectStore, instanceID string, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if countTraceObjects(t, store, instanceID) >= want {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func countTraceObjects(t testing.TB, store jetstream.ObjectStore, instanceID string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	infos, err := store.List(ctx)
	if err != nil {
		return 0 // empty store reports an error; treat as zero objects
	}
	n := 0
	for _, info := range infos {
		if info.Metadata["instance_id"] == instanceID {
			n++
		}
	}
	return n
}
