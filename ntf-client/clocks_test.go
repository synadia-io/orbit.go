package ntf

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/events"
)

// startShiftedClock answers clock requests on a connection of its own to the
// test service's management server, as another component would, with a clock
// running shift ahead of the local one. It returns the clock subject, unique to
// the test, and stops answering when the test ends.
func startShiftedClock(t *testing.T, shift time.Duration) string {
	t.Helper()

	nc, err := nats.Connect(testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	group := fmt.Sprintf("ntfclientclock%d", time.Now().UnixNano())
	svc, err := micro.AddService(nc, micro.Config{Name: group, Version: "0.0.1"})
	if err != nil {
		t.Fatalf("add service: %v", err)
	}
	t.Cleanup(func() { svc.Stop() })

	handler := &events.ClockHandler{Now: func() time.Time { return time.Now().Add(shift) }}
	err = svc.AddGroup(group).AddEndpoint(events.ClockEndpoint, handler)
	if err != nil {
		t.Fatalf("add endpoint: %v", err)
	}
	// The service probes on a connection of its own, so the subscription has
	// to reach the server first.
	err = nc.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}

	return group + "." + events.ClockEndpoint
}

// findClock returns the participant of kind with the instance id or subject
// key, or nil.
func findClock(resp *api.ClockResponse, kind string, key string) *api.ClockParticipant {
	for i, p := range resp.Participants {
		if p.Kind == kind && (p.InstanceID == key || p.Subject == key) {
			return &resp.Participants[i]
		}
	}
	return nil
}

// TestClocksReportsParticipants proves Manager.Clocks reports the service as
// the reference, the agent of a traced instance as sharing its clock, a
// shifted clock with its shift within the bound, and an error for a clock
// subject nothing answers.
func TestClocksReportsParticipants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m := connectEvents(t, ctx)
	inst, err := m.CreateServer(ctx, false, WithDescription(t.Name()), WithTraceCapture())
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer inst.Destroy(t)

	shift := 2 * time.Second
	shifted := startShiftedClock(t, shift)
	silent := shifted + ".nobody"

	resp, err := m.Clocks(ctx, shifted, silent)
	if err != nil {
		t.Fatalf("clocks: %v", err)
	}

	if len(resp.Participants) == 0 || resp.Participants[0].Kind != api.ClockService || !resp.Participants[0].Reference {
		t.Fatalf("participants %+v, want the service first as the reference", resp.Participants)
	}

	agent := findClock(resp, api.ClockAgent, inst.ID)
	if agent == nil {
		t.Fatalf("no agent reported for %s: %+v", inst.ID, resp.Participants)
	}
	if !agent.SharedClock || agent.Offset != 0 {
		t.Errorf("agent %+v, want a shared clock with no offset", agent)
	}

	answered := findClock(resp, api.ClockExtra, shifted)
	if answered == nil {
		t.Fatalf("no participant for %s: %+v", shifted, resp.Participants)
	}
	if answered.Error != "" {
		t.Fatalf("%s reported error %q", shifted, answered.Error)
	}
	diff := (answered.Offset - shift).Abs()
	if diff > answered.Bound {
		t.Errorf("offset %v is %v from the shift %v, beyond the bound %v", answered.Offset, diff, shift, answered.Bound)
	}

	unanswered := findClock(resp, api.ClockExtra, silent)
	if unanswered == nil {
		t.Fatalf("no participant for %s: %+v", silent, resp.Participants)
	}
	if unanswered.Error == "" {
		t.Errorf("%s reported no error: %+v", silent, unanswered)
	}
}

// recordingTB records the failures AssertClocksWithin reports instead of
// failing the test that runs it.
type recordingTB struct {
	testing.TB
	failures []string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

// TestAssertClocksWithin proves AssertClocksWithin passes for the service and
// the agent running in its process, and fails for a clock shifted beyond the
// bound and for a clock subject nothing answers.
func TestAssertClocksWithin(t *testing.T) {
	client := New(t, testServerURL())
	defer client.Close(t)

	inst := client.CreateServer(t, false, WithTraceCapture())
	defer inst.Destroy(t)

	client.AssertClocksWithin(t, time.Second)

	shifted := startShiftedClock(t, 5*time.Second)
	cases := map[string]struct {
		subject string
		reason  string
	}{
		"shifted":    {shifted, "exceeds 1s"},
		"unanswered": {shifted + ".nobody", events.ErrClockProbe.Error()},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := &recordingTB{TB: t}
			client.AssertClocksWithin(rec, time.Second, tc.subject)

			if len(rec.failures) != 1 {
				t.Fatalf("got %d failures, want 1: %v", len(rec.failures), rec.failures)
			}
			if !strings.Contains(rec.failures[0], tc.subject) {
				t.Errorf("failure %q does not name %s", rec.failures[0], tc.subject)
			}
			if !strings.Contains(rec.failures[0], tc.reason) {
				t.Errorf("failure %q does not say %q", rec.failures[0], tc.reason)
			}
		})
	}
}
