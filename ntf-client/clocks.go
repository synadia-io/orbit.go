package ntf

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// Clocks asks the management service for the clocks of every participant
// against the service host's clock: the service itself, the reference, the
// agent of each instance that has one, and each subject in extra. An extra
// subject is the clock endpoint of another component, such as a workload
// engine, which the service probes; one that does not answer is reported with
// its Error set rather than failing the call. The response holds no verdict.
func (m *Manager) Clocks(ctx context.Context, extra ...string) (*api.ClockResponse, error) {
	resp := api.ClockResponse{}
	err := m.request(ctx, "tester.clock", requestTimeout, api.ClockRequest{Extra: extra}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// AssertClocksWithin fails the test unless every participant's clock is within
// d of the service host's: it fails when a participant reports an error, or
// when the size of its offset plus its error bound exceeds d. extra lists the
// clock subjects of other components to check, as Manager.Clocks takes them.
// Call it at the start of a test or flow whose events are lined up by time.
func (c *Client) AssertClocksWithin(t testing.TB, d time.Duration, extra ...string) {
	t.Helper()

	resp, err := c.m.Clocks(context.Background(), extra...)
	if err != nil {
		t.Fatalf("could not check clocks: %v", err)
		// A testing.TB whose Fatalf does not stop the test returns here.
		return
	}

	var failed []string
	for _, p := range resp.Participants {
		if p.Error != "" {
			failed = append(failed, fmt.Sprintf("%s: %s", clockName(p), p.Error))
			continue
		}
		if p.Offset.Abs()+p.Bound > d {
			failed = append(failed, fmt.Sprintf("%s: offset %v with bound %v exceeds %v", clockName(p), p.Offset, p.Bound, d))
		}
	}

	if len(failed) > 0 {
		t.Fatalf("clocks not within %v: %s", d, strings.Join(failed, "; "))
	}
}

// clockName names a participant for a failure message.
func clockName(p api.ClockParticipant) string {
	switch {
	case p.InstanceID != "":
		return fmt.Sprintf("%s of instance %s", p.Kind, p.InstanceID)
	case p.Subject != "":
		return fmt.Sprintf("%s %s", p.Kind, p.Subject)
	default:
		return p.Kind
	}
}
