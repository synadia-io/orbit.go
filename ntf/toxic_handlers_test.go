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
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/synadia-io/orbit.go/ntf/agent"
	"github.com/synadia-io/orbit.go/ntf/api"
)

var _ agent.Toxics = (*fakeAgent)(nil)

func (a *fakeAgent) AddToxic(_ context.Context, toxic api.Toxic) (api.ActiveToxic, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.toxicErr != nil {
		return api.ActiveToxic{}, a.toxicErr
	}
	a.added = append(a.added, toxic)

	return api.ActiveToxic{Toxic: toxic, Links: []string{"route:a->b"}, Since: time.Unix(1, 0).UTC()}, nil
}

func (a *fakeAgent) RemoveToxic(_ context.Context, name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.toxicErr != nil {
		return a.toxicErr
	}
	a.removed = append(a.removed, name)

	return nil
}

func (a *fakeAgent) ClearToxics(context.Context) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.toxicErr != nil {
		return nil, a.toxicErr
	}

	var names []string
	for _, tx := range a.added {
		names = append(names, tx.Name)
	}
	return names, nil
}

func (a *fakeAgent) ToxicReport(context.Context) ([]api.LinkToxics, []api.ActiveToxic, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.toxicErr != nil {
		return nil, nil, a.toxicErr
	}

	var active []api.ActiveToxic
	for _, tx := range a.added {
		active = append(active, api.ActiveToxic{Toxic: tx, Links: []string{"route:a->b"}, Since: time.Unix(1, 0).UTC()})
	}
	links := []api.LinkToxics{{Link: api.Link{Name: "route:a->b", Kind: "route"}}}

	return links, active, nil
}

// TestToxicCallsReachTheAgent proves each tester.toxic endpoint hands its
// request to the agent of an instance created with toxics and answers with
// what the agent returned.
func TestToxicCallsReachTheAgent(t *testing.T) {
	fake := &fakeAgent{}
	svc := startTestService(t, withAgent(&fakeProvider{toxics: true, agent: fake}))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 2, Toxics: true}, &created)

	toxicity := 0.5
	toxic := api.Toxic{
		Name:       "slow",
		Selector:   api.ToxicSelector{Kind: api.ToxicKindRoute, From: "a", To: "b", Direction: api.ToxicDirectionTo},
		Type:       api.ToxicLatency,
		Attributes: map[string]any{"latency": float64(100)},
		Toxicity:   &toxicity,
	}

	var added api.ToxicAddResponse
	mustRequest(t, svc.nc, "tester.toxic.add", api.ToxicAddRequest{InstanceID: created.ID, Toxic: toxic}, &added)
	if !reflect.DeepEqual(added.Toxic.Toxic, toxic) || !slices.Equal(added.Toxic.Links, []string{"route:a->b"}) {
		t.Errorf("add response = %+v, want the toxic and its links", added.Toxic)
	}

	fake.mu.Lock()
	got := slices.Clone(fake.added)
	fake.mu.Unlock()
	if len(got) != 1 || !reflect.DeepEqual(got[0], toxic) {
		t.Errorf("agent was handed %+v, want %+v", got, toxic)
	}

	var report api.ToxicReportResponse
	mustRequest(t, svc.nc, "tester.toxic.report", api.ToxicReportRequest{InstanceID: created.ID}, &report)
	if len(report.Links) != 1 || report.Links[0].Name != "route:a->b" || len(report.Toxics) != 1 || report.Toxics[0].Name != "slow" {
		t.Errorf("report = %+v, want the agent's link and toxic", report)
	}

	var removed api.ToxicRemoveResponse
	mustRequest(t, svc.nc, "tester.toxic.remove", api.ToxicRemoveRequest{InstanceID: created.ID, Name: "slow"}, &removed)
	if removed.Removed != "slow" {
		t.Errorf("remove response = %+v, want slow", removed)
	}

	var cleared api.ToxicClearResponse
	mustRequest(t, svc.nc, "tester.toxic.clear", api.ToxicClearRequest{InstanceID: created.ID}, &cleared)
	if !slices.Equal(cleared.Cleared, []string{"slow"}) {
		t.Errorf("clear response = %+v, want [slow]", cleared)
	}

	fake.mu.Lock()
	removedNames := slices.Clone(fake.removed)
	fake.mu.Unlock()
	if !slices.Equal(removedNames, []string{"slow"}) {
		t.Errorf("agent removed %v, want [slow]", removedNames)
	}
}

// TestToxicCallsAnswerEmptyLists proves clear and report answer empty lists
// rather than null when the agent returns none.
func TestToxicCallsAnswerEmptyLists(t *testing.T) {
	svc := startTestService(t, withAgent(&fakeProvider{toxics: true, agent: &fakeAgent{}}))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Toxics: true}, &created)

	msg, err := svc.nc.Request("tester.toxic.clear", []byte(`{"instance_id":"`+created.ID+`"}`), 10*time.Second)
	if err != nil {
		t.Fatalf("clear: %v", err)
	}
	if !strings.Contains(string(msg.Data), `"cleared":[]`) {
		t.Errorf("clear response = %s, want an empty cleared list", msg.Data)
	}
}

// TestStatusListsToxics proves status lists the toxics the agent of an
// instance created with toxics reports, with their links and since when, none
// for an instance without toxics, and none, without failing, when the agent
// cannot report them.
func TestStatusListsToxics(t *testing.T) {
	fake := &fakeAgent{}
	provider := &fakeProvider{toxics: true, agent: fake}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 2, Toxics: true}, &created)
	var plain api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Proxy: true}, &plain)

	provider.mu.Lock()
	starts := provider.starts
	provider.mu.Unlock()
	if starts != 2 {
		t.Fatalf("the provider started %d agents, want one for each instance", starts)
	}

	status := func(id string) api.InstanceStatus {
		t.Helper()

		var resp api.StatusResponse
		mustRequest(t, svc.nc, "tester.status", api.StatusRequest{InstanceID: id}, &resp)
		if len(resp.Instances) != 1 {
			t.Fatalf("status lists %d instances, want 1", len(resp.Instances))
		}
		return resp.Instances[0]
	}

	got := status(created.ID).Toxics
	if len(got) != 0 {
		t.Fatalf("status before any toxic lists %+v", got)
	}

	toxic := api.Toxic{Name: "slow", Selector: api.ToxicSelector{Kind: api.ToxicKindRoute}, Type: api.ToxicLatency}
	var added api.ToxicAddResponse
	mustRequest(t, svc.nc, "tester.toxic.add", api.ToxicAddRequest{InstanceID: created.ID, Toxic: toxic}, &added)

	want := []api.ActiveToxic{{Toxic: toxic, Links: []string{"route:a->b"}, Since: time.Unix(1, 0).UTC()}}
	got = status(created.ID).Toxics
	if !reflect.DeepEqual(got, want) {
		t.Errorf("status toxics = %+v, want %+v", got, want)
	}

	// The proxied instance has the same agent, which reports slow, but no
	// links, so status does not ask it.
	plainStatus := status(plain.ID)
	if len(plainStatus.Links) != 0 {
		t.Fatalf("plain instance status = %+v, want no links", plainStatus)
	}
	if len(plainStatus.Toxics) != 0 {
		t.Errorf("status of an instance without toxics lists %+v", plainStatus.Toxics)
	}

	fake.mu.Lock()
	fake.toxicErr = errors.New("report failed")
	fake.mu.Unlock()

	failed := status(created.ID)
	if len(failed.Toxics) != 0 {
		t.Errorf("status with a failing report lists %+v", failed.Toxics)
	}
	if len(failed.Servers) != 2 {
		t.Errorf("status with a failing report lists %d servers, want 2", len(failed.Servers))
	}
}

// TestToxicCallsRefusals proves every refusal of the tester.toxic endpoints
// and its code: 001 for a request that does not decode, 002 without an
// instance id, 014 without an agent, 018 when the agent does not declare
// toxics, 404 for an unknown instance, 020 for an instance created without
// toxics, 021 for a call the agent refused for what it asked, and 022 for any
// other agent failure and for an agent that does not implement agent.Toxics.
func TestToxicCallsRefusals(t *testing.T) {
	invalid := &fakeAgent{toxicErr: fmt.Errorf("%w: toxic \"x\": latency attribute \"bogus\" is unknown", agent.ErrInvalidToxic)}
	invalidSvc := startTestService(t, withAgent(&fakeProvider{toxics: true, agent: invalid}))
	var invalidInst api.CreateResponse
	mustRequest(t, invalidSvc.nc, "tester.create.server", api.CreateServerRequest{Toxics: true}, &invalidInst)

	failing := &fakeAgent{toxicErr: errors.New("proxy fell over")}
	failingSvc := startTestService(t, withAgent(&fakeProvider{toxics: true, agent: failing}))
	var failingInst api.CreateResponse
	mustRequest(t, failingSvc.nc, "tester.create.server", api.CreateServerRequest{Toxics: true}, &failingInst)

	hidden := startTestService(t, withAgent(&fakeProvider{toxics: true, hideToxics: true}))
	var hiddenInst api.CreateResponse
	mustRequest(t, hidden.nc, "tester.create.server", api.CreateServerRequest{Toxics: true}, &hiddenInst)

	toxicsSvc := startTestService(t, withAgent(&fakeProvider{toxics: true, agent: &fakeAgent{}}))
	var plainInst api.CreateResponse
	mustRequest(t, toxicsSvc.nc, "tester.create.server", api.CreateServerRequest{Proxy: true}, &plainInst)

	noAgent := startTestService(t)
	noToxics := startTestService(t, withAgent(&fakeProvider{}))

	tests := []struct {
		name     string
		svc      *Service
		id       string
		code     string
		contains string
	}{
		{"no instance id", toxicsSvc, "", "002", "instance_id is required"},
		{"no agent", noAgent, "any", "014", "toxics is not configured"},
		{"agent without toxics", noToxics, "any", "018", `"toxics"`},
		{"unknown instance", toxicsSvc, "nope", "404", "Instance not found"},
		{"instance without toxics", toxicsSvc, plainInst.ID, "020", "no link proxies"},
		{"refused by the agent", invalidSvc, invalidInst.ID, "021", `latency attribute "bogus" is unknown`},
		{"agent failure", failingSvc, failingInst.ID, "022", "proxy fell over"},
		{"agent without the interface", hidden, hiddenInst.ID, "022", "agent.Toxics"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqs := map[string]any{
				"tester.toxic.add":    api.ToxicAddRequest{InstanceID: tt.id, Toxic: api.Toxic{Name: "x", Type: api.ToxicLatency}},
				"tester.toxic.remove": api.ToxicRemoveRequest{InstanceID: tt.id, Name: "x"},
				"tester.toxic.clear":  api.ToxicClearRequest{InstanceID: tt.id},
				"tester.toxic.report": api.ToxicReportRequest{InstanceID: tt.id},
			}
			for subject, req := range reqs {
				code, msg := serviceError(t, tt.svc.nc, subject, req)
				if code != tt.code {
					t.Errorf("%s code = %q (%s), want %s", subject, code, msg, tt.code)
				}
				if !strings.Contains(msg, tt.contains) {
					t.Errorf("%s message %q does not contain %q", subject, msg, tt.contains)
				}
			}
		})
	}

	for _, subject := range []string{"tester.toxic.add", "tester.toxic.remove", "tester.toxic.clear", "tester.toxic.report"} {
		msg, err := toxicsSvc.nc.Request(subject, []byte("{"), 10*time.Second)
		if err != nil {
			t.Fatalf("request %s: %v", subject, err)
		}
		code := msg.Header.Get("Nats-Service-Error-Code")
		if code != "001" {
			t.Errorf("%s with a request that does not decode: code %q, want 001", subject, code)
		}
	}
}

// TestToxicsSpecCarriesNodes proves the agent of an instance created with
// toxics is handed every node with its cluster, and the agent of one without
// toxics none.
func TestToxicsSpecCarriesNodes(t *testing.T) {
	provider := &fakeProvider{toxics: true}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.super-cluster", api.CreateSuperClusterRequest{Clusters: 2, Servers: 2, Toxics: true}, &created)
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Proxy: true}, nil)

	specs, _ := provider.snapshot()
	if len(specs) != 2 {
		t.Fatalf("provider was handed %d specs, want 2", len(specs))
	}

	var want []agent.Node
	for _, srv := range created.Servers {
		want = append(want, agent.Node{Name: srv.Name, Cluster: srv.Cluster})
	}
	if !reflect.DeepEqual(specs[0].Nodes, want) {
		t.Errorf("Spec.Nodes = %+v, want %+v", specs[0].Nodes, want)
	}
	if len(specs[1].Nodes) != 0 {
		t.Errorf("Spec.Nodes without toxics = %+v, want none", specs[1].Nodes)
	}
}
