package ntf

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/ntf/api"
)

// TestShapingRoundTrip pins set, report and clear through the testing.TB methods:
// a set shows on the instance status, drops the first matching MSG for the named
// connection only, reports that one firing, and is gone after a clear.
func TestShapingRoundTrip(t *testing.T) {
	client := New(t, testServerURL())
	defer client.Close(t)

	inst := client.CreateServer(t, false, WithTraceCapture())
	defer inst.Destroy(t)

	const name = "ntf-client/shaping"
	set := api.ShapingSet{
		ID:             "drop-first-demo",
		ConnectionName: "^" + name + "$",
		Rules: []api.ShapingRule{{
			ID: "drop-demo",
			Match: api.ShapingMatch{
				Direction: "from_server",
				Verb:      []string{"MSG"},
				Subject:   &api.SubjectMatch{Exact: "demo"},
			},
			Action: api.ShapingAction{Kind: api.ShapingDrop},
			Limit:  1,
		}},
	}

	resp := inst.SetShaping(t, set)
	if resp.Set != set.ID {
		t.Fatalf("set response = %q, want %q", resp.Set, set.ID)
	}
	if !slices.Contains(inst.Status(t).ShapingSets, set.ID) {
		t.Fatalf("status shaping sets = %v, want %s", inst.Status(t).ShapingSets, set.ID)
	}

	nc, err := nats.Connect(inst.Servers[0].TraceURL, nats.Name(name))
	if err != nil {
		t.Fatalf("connect via trace url: %v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync("demo")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	for _, body := range []string{"one", "two"} {
		err = nc.Publish("demo", []byte(body))
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	err = nc.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("next msg: %v", err)
	}
	if string(msg.Data) != "two" {
		t.Fatalf("first delivered message = %q, want two: the rule should have dropped one", msg.Data)
	}

	report := inst.ShapingReport(t, set.ID)
	if len(report.Sets) != 1 || report.Sets[0].ID != set.ID {
		t.Fatalf("report sets = %+v, want only %s", report.Sets, set.ID)
	}
	firings := report.Sets[0].Firings
	if len(firings) != 1 {
		t.Fatalf("firings = %+v, want exactly one", firings)
	}
	if firings[0].Rule != "drop-demo" || firings[0].Action != api.ShapingDrop || firings[0].ClientName != name {
		t.Fatalf("firing = %+v, want rule drop-demo, action drop, client %s", firings[0], name)
	}

	cleared := inst.ClearShaping(t, "")
	if !slices.Equal(cleared.Cleared, []string{set.ID}) {
		t.Fatalf("cleared = %v, want [%s]", cleared.Cleared, set.ID)
	}
	if len(inst.Status(t).ShapingSets) != 0 {
		t.Fatalf("status shaping sets after clear = %v, want none", inst.Status(t).ShapingSets)
	}
}

// TestManagerShapingNeedsProxy pins that the Manager returns the service's refusal
// of a shaping set on an instance without trace capture as ErrService, rather than
// failing anything.
func TestManagerShapingNeedsProxy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer m.Close()

	inst, err := m.CreateServer(ctx, false, WithDescription(t.Name()))
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer inst.Destroy(t)

	_, err = m.SetShaping(ctx, inst.ID, api.ShapingSet{ID: "any", ConnectionName: ".*"})
	if !errors.Is(err, ErrService) {
		t.Fatalf("set shaping without a proxy: err = %v, want ErrService", err)
	}
}
