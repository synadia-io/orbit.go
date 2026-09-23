package ntf

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/ntf/api"
)

// TestCapturesReconnectingClient pins that one client that reconnects produces two
// captures under its CONNECT name, that Captures waits for the second to land,
// returns both in the order their connections closed with their metadata, leaves
// out another client on the same instance, and reads each one back.
func TestCapturesReconnectingClient(t *testing.T) {
	client := New(t, testServerURL())
	defer client.Close(t)

	inst := client.CreateServer(t, false, WithTraceCapture())
	defer inst.Destroy(t)

	const name = "ntf-client/reconnect"
	const other = "ntf-client/other"

	// The proxy closes the connection when it sees this PUB, and the client
	// reconnects through it.
	inst.SetShaping(t, api.ShapingSet{
		ID:             "disconnect-once",
		ConnectionName: "^" + name + "$",
		Rules: []api.ShapingRule{{
			ID: "disconnect",
			Match: api.ShapingMatch{
				Direction: "to_server",
				Verb:      []string{"PUB"},
				Subject:   &api.SubjectMatch{Exact: "reconnect.now"},
			},
			Action: api.ShapingAction{Kind: api.ShapingDisconnect},
		}},
	})

	onc, err := nats.Connect(inst.Servers[0].TraceURL, nats.Name(other))
	if err != nil {
		t.Fatalf("connect %s: %v", other, err)
	}
	err = onc.Flush()
	if err != nil {
		t.Fatalf("flush %s: %v", other, err)
	}
	onc.Close()

	reconnected := make(chan struct{}, 1)
	nc, err := nats.Connect(inst.Servers[0].TraceURL,
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(50*time.Millisecond),
		nats.ReconnectHandler(func(*nats.Conn) { reconnected <- struct{}{} }))
	if err != nil {
		t.Fatalf("connect %s: %v", name, err)
	}
	defer nc.Close()

	err = nc.Publish("reconnect.now", []byte("bye"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not reconnect after the proxy disconnected it")
	}
	err = nc.Flush()
	if err != nil {
		t.Fatalf("flush after reconnect: %v", err)
	}
	nc.Close()

	captures := inst.Captures(t, name, 2, 10*time.Second)
	if len(captures) != 2 {
		t.Fatalf("captures for %s = %d, want 2", name, len(captures))
	}
	for _, c := range captures {
		if c.Info.Metadata["client_name"] != name {
			t.Errorf("capture %s client_name = %q, want %q", c.Info.Name, c.Info.Metadata["client_name"], name)
		}
		if c.Info.Metadata["instance_id"] != inst.ID {
			t.Errorf("capture %s instance_id = %q, want %q", c.Info.Name, c.Info.Metadata["instance_id"], inst.ID)
		}
	}
	first, second := captures[0], captures[1]
	if first.Info.Metadata["connection_uuid"] == second.Info.Metadata["connection_uuid"] {
		t.Errorf("both captures carry connection_uuid %q, want one per connection", first.Info.Metadata["connection_uuid"])
	}
	if second.CapturedAt.Before(first.CapturedAt) {
		t.Errorf("captures out of order: %s before %s", first.CapturedAt, second.CapturedAt)
	}
	if first.Info.Metadata["shaped"] != "true" || second.Info.Metadata["shaped"] != "false" {
		t.Errorf("shaped = %q then %q, want true for the disconnected connection then false",
			first.Info.Metadata["shaped"], second.Info.Metadata["shaped"])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data, err := first.Read(ctx)
	if err != nil {
		t.Fatalf("read %s: %v", first.Info.Name, err)
	}
	if !strings.Contains(string(data), "reconnect.now") {
		t.Errorf("first capture does not hold the PUB that disconnected it")
	}
	data, err = second.Read(ctx)
	if err != nil {
		t.Fatalf("read %s: %v", second.Info.Name, err)
	}
	if len(data) == 0 {
		t.Errorf("second capture is empty")
	}

	// The other client's capture is on the same instance, so leaving it out of
	// the list above was the name filter at work.
	if len(inst.Captures(t, other, 1, 10*time.Second)) != 1 {
		t.Errorf("want exactly one capture for %s", other)
	}
}

// TestManagerCapturesWait pins the wait: with no captures for a name, a want of
// zero returns at once with none, and a positive want ends with ErrCaptureWait
// when the context does.
func TestManagerCapturesWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer m.Close()

	inst, err := m.CreateServer(ctx, false, WithTraceCapture(), WithDescription(t.Name()))
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer inst.Destroy(t)

	captures, err := m.Captures(ctx, inst.ID, "ntf-client/never", 0)
	if err != nil {
		t.Fatalf("captures with want 0: %v", err)
	}
	if len(captures) != 0 {
		t.Fatalf("captures with want 0 = %d, want none", len(captures))
	}

	wctx, wcancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer wcancel()
	_, err = m.Captures(wctx, inst.ID, "ntf-client/never", 1)
	if !errors.Is(err, ErrCaptureWait) {
		t.Fatalf("captures with want 1: err = %v, want ErrCaptureWait", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("captures with want 1: err = %v, want it to wrap context.DeadlineExceeded", err)
	}

	captures = inst.Captures(t, "ntf-client/never", 0, 0)
	if len(captures) != 0 {
		t.Fatalf("wrapper captures with want 0 and wait 0 = %d, want none", len(captures))
	}
}

// TestManagerInstanceNotFound pins that the service's 404 for an unknown
// instance reaches the caller as ErrInstanceNotFound as well as ErrService.
func TestManagerInstanceNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer m.Close()

	_, err = m.Destroy(ctx, "no-such-instance")
	if !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("destroy unknown instance: err = %v, want ErrInstanceNotFound", err)
	}
	if !errors.Is(err, ErrService) {
		t.Fatalf("destroy unknown instance: err = %v, want ErrService", err)
	}
}
