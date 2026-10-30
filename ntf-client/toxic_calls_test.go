package ntf

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/ntf/api"
)

// TestToxicRoundTrip pins add, report, remove and clear through the testing.TB
// methods: a toxic is applied to the links its selector names, shows on them in
// the report, and is gone after a remove, and a clear removes the rest.
func TestToxicRoundTrip(t *testing.T) {
	client := New(t, testServerURL())
	defer client.Close(t)

	inst := client.CreateCluster(t, 2, false, WithToxics())
	defer inst.Destroy(t)

	a, b := inst.Servers[0].Name, inst.Servers[1].Name
	added := inst.AddToxic(t, api.Toxic{
		Name:       "slow",
		Selector:   api.ToxicSelector{Kind: api.ToxicKindRoute, From: a, To: b, Direction: api.ToxicDirectionTo},
		Type:       api.ToxicLatency,
		Attributes: map[string]any{"latency": 5},
	})
	wantLinks := []string{"route:" + a + "->" + b, "route:" + b + "->" + a}
	if !slices.Equal(added.Toxic.Links, wantLinks) {
		t.Errorf("slow resolved to %v, want %v", added.Toxic.Links, wantLinks)
	}
	if added.Toxic.Toxicity == nil || *added.Toxic.Toxicity != 1 {
		t.Errorf("slow toxicity = %v, want 1", added.Toxic.Toxicity)
	}

	inst.AddToxic(t, api.Toxic{Name: "thin", Selector: api.ToxicSelector{Kind: api.ToxicKindClient, To: a}, Type: api.ToxicBandwidth, Attributes: map[string]any{"rate": 1000}})

	report := inst.ToxicReport(t)
	if len(report.Links) != len(inst.Links) {
		t.Errorf("report lists %d links, want the instance's %d", len(report.Links), len(inst.Links))
	}
	on := map[string]string{}
	for _, lk := range report.Links {
		for _, tx := range lk.Toxics {
			on[lk.Name] += tx.Name + "/" + tx.Direction + " "
		}
	}
	want := map[string]string{
		"route:" + a + "->" + b: "slow/to ",
		"route:" + b + "->" + a: "slow/from ",
		"client:" + a:           "thin/both ",
	}
	for link, toxics := range want {
		if on[link] != toxics {
			t.Errorf("link %s carries %q, want %q", link, on[link], toxics)
		}
	}
	if len(on) != len(want) {
		t.Errorf("toxics on links = %v, want %v", on, want)
	}

	removed := inst.RemoveToxic(t, "slow")
	if removed.Removed != "slow" {
		t.Errorf("removed = %q, want slow", removed.Removed)
	}

	cleared := inst.ClearToxics(t)
	if !slices.Equal(cleared.Cleared, []string{"thin"}) {
		t.Errorf("cleared = %v, want [thin]", cleared.Cleared)
	}
	if len(inst.ToxicReport(t).Toxics) != 0 {
		t.Errorf("toxics after clear = %v, want none", inst.ToxicReport(t).Toxics)
	}
}

// TestUnplug pins that Unplug cuts a server off, including its client link, so a
// client can no longer reach it, and that restore lets clients in again.
func TestUnplug(t *testing.T) {
	client := New(t, testServerURL())
	defer client.Close(t)

	inst := client.CreateCluster(t, 3, false, WithToxics())
	defer inst.Destroy(t)

	srv := inst.Servers[2]
	restore := inst.Unplug(t, srv)

	report := inst.ToxicReport(t)
	if len(report.Toxics) != 1 || report.Toxics[0].Name != "unplug-"+srv.Name || report.Toxics[0].Type != api.ToxicCut {
		t.Fatalf("toxics = %+v, want the unplug of %s", report.Toxics, srv.Name)
	}

	nc, err := nats.Connect(srv.URL, nats.Timeout(time.Second), nats.NoReconnect())
	if err == nil {
		nc.Close()
		t.Fatalf("connected to the unplugged %s", srv.Name)
	}

	restore()

	nc, err = nats.Connect(srv.URL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect to %s after restore: %v", srv.Name, err)
	}
	defer nc.Close()
	if nc.ConnectedServerName() != srv.Name {
		t.Errorf("reached %s, want %s", nc.ConnectedServerName(), srv.Name)
	}
	if len(inst.ToxicReport(t).Toxics) != 0 {
		t.Error("the unplug is still in place after restore")
	}
}

// TestManagerToxicRefusals pins that the Manager returns the service's
// refusals as ErrService: a toxic on an instance created without toxics, and an
// attribute the request schema does not accept, named in the error.
func TestManagerToxicRefusals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer m.Close()

	plain, err := m.CreateServer(ctx, false, WithDescription(t.Name()))
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer plain.Destroy(t)

	_, err = m.AddToxic(ctx, plain.ID, api.Toxic{Name: "x", Type: api.ToxicCut})
	if !errors.Is(err, ErrService) || !strings.Contains(err.Error(), "no link proxies") {
		t.Errorf("toxic on an instance without toxics: err = %v, want ErrService naming the missing proxies", err)
	}

	proxied, err := m.CreateServer(ctx, false, WithToxics(), WithDescription(t.Name()))
	if err != nil {
		t.Fatalf("create server with toxics: %v", err)
	}
	defer proxied.Destroy(t)

	_, err = m.AddToxic(ctx, proxied.ID, api.Toxic{Name: "x", Type: api.ToxicLatency, Attributes: map[string]any{"latency": -1}})
	if !errors.Is(err, ErrService) || !strings.Contains(err.Error(), "Invalid request: toxic.attributes.latency: minimum: got -1, want 0") {
		t.Errorf("toxic with a negative latency: err = %v, want ErrService naming the attribute path and its minimum", err)
	}

	_, err = m.RemoveToxic(ctx, proxied.ID, "nope")
	if !errors.Is(err, ErrService) {
		t.Errorf("removing an unknown toxic: err = %v, want ErrService", err)
	}

	cleared, err := m.ClearToxics(ctx, proxied.ID)
	if err != nil {
		t.Fatalf("clear: %v", err)
	}
	if len(cleared.Cleared) != 0 {
		t.Errorf("cleared = %v, want none", cleared.Cleared)
	}
}
