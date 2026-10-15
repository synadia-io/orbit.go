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
	"testing"
	"time"

	"github.com/nats-io/nats.go/micro"

	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/events"
)

// TestClockReport proves tester.clock reports the service as the reference,
// the agent of each live instance that has one as sharing the service's clock,
// an extra subject answered by a clock running ahead with that offset within
// its bound, and an extra subject nothing answers with an error.
func TestClockReport(t *testing.T) {
	svc := startTestService(t, withAgent(&fakeProvider{}))

	var traced api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Trace: true}, &traced)
	var untraced api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{}, &untraced)

	shift := 1500 * time.Millisecond
	workload, err := micro.AddService(svc.nc, micro.Config{Name: "workload", Version: "0.0.1"})
	if err != nil {
		t.Fatalf("add service: %v", err)
	}
	t.Cleanup(func() { workload.Stop() })
	err = workload.AddGroup("workload").AddEndpoint(events.ClockEndpoint, &events.ClockHandler{Now: func() time.Time { return time.Now().Add(shift) }})
	if err != nil {
		t.Fatalf("add endpoint: %v", err)
	}

	var resp api.ClockResponse
	mustRequest(t, svc.nc, "tester.clock", api.ClockRequest{Extra: []string{"workload.clock", "nobody.clock"}}, &resp)

	if len(resp.Participants) != 4 {
		t.Fatalf("got %d participants, want 4 (service, one agent, two extra): %+v", len(resp.Participants), resp.Participants)
	}

	service := resp.Participants[0]
	if service.Kind != api.ClockService || !service.Reference {
		t.Errorf("first participant %+v, want the service as the reference", service)
	}
	if service.Offset != 0 || service.Bound != 0 || service.Error != "" {
		t.Errorf("service %+v, want no offset, bound or error", service)
	}

	agent := resp.Participants[1]
	if agent.Kind != api.ClockAgent || agent.InstanceID != traced.ID || !agent.SharedClock {
		t.Errorf("second participant %+v, want the agent of %s sharing the service's clock", agent, traced.ID)
	}

	answered := resp.Participants[2]
	if answered.Kind != api.ClockExtra || answered.Subject != "workload.clock" {
		t.Fatalf("third participant %+v, want extra workload.clock", answered)
	}
	if answered.Error != "" {
		t.Fatalf("workload.clock reported error %q", answered.Error)
	}
	if answered.RoundTrip <= 0 || answered.Bound != answered.RoundTrip/2 {
		t.Errorf("workload.clock round trip %v and bound %v, want a positive round trip and half of it", answered.RoundTrip, answered.Bound)
	}
	diff := (answered.Offset - shift).Abs()
	if diff > answered.Bound {
		t.Errorf("workload.clock offset %v is %v from the shift %v, beyond the bound %v", answered.Offset, diff, shift, answered.Bound)
	}

	silent := resp.Participants[3]
	if silent.Kind != api.ClockExtra || silent.Subject != "nobody.clock" {
		t.Fatalf("fourth participant %+v, want extra nobody.clock", silent)
	}
	if silent.Error == "" {
		t.Errorf("nobody.clock reported no error: %+v", silent)
	}
}

// TestClockReportEmptyRequest proves an empty request reports the service
// alone when no instance has an agent.
func TestClockReportEmptyRequest(t *testing.T) {
	svc := startTestService(t)

	var resp api.ClockResponse
	mustRequest(t, svc.nc, "tester.clock", nil, &resp)

	if len(resp.Participants) != 1 || resp.Participants[0].Kind != api.ClockService {
		t.Errorf("participants %+v, want the service alone", resp.Participants)
	}
}
