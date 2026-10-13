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
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/synadia-io/orbit.go/ntf/agent"
	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/backend"
)

// startTestService starts a service on an embedded server with an OS-chosen port
// and instance state under a temp dir, and closes it when the test ends.
func startTestService(t *testing.T, opts ...func(*Options)) *Service {
	t.Helper()

	o := Options{Dir: t.TempDir()}
	for _, opt := range opts {
		opt(&o)
	}

	svc, err := New(t.Context(), o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return svc
}

func TestNewDefaults(t *testing.T) {
	svc := startTestService(t)

	if svc.EmbeddedServer() == nil {
		t.Fatal("expected an embedded server")
	}
	if svc.Port() == 0 {
		t.Error("expected a bound port")
	}
	if svc.ClientURL() == "" {
		t.Error("expected a client URL")
	}
	if svc.ManagementConn() == nil {
		t.Fatal("expected a management connection")
	}
}

// TestNewOwnsTempDir proves an empty Options.Dir gets a service-owned directory
// that Close removes, and that Dir reports it so a caller can find it meanwhile.
func TestNewOwnsTempDir(t *testing.T) {
	svc, err := New(t.Context(), Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	dir := svc.Dir()
	if dir == "" {
		t.Fatal("Dir returned nothing")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("stat %s: %v", dir, err)
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("Close left %s behind (err %v)", dir, err)
	}
}

// TestNewSuppliedConn proves the service hosts itself on a connection the caller
// owns, answers requests on it, and leaves it open after Close.
func TestNewSuppliedConn(t *testing.T) {
	host := startTestService(t)
	nc := host.ManagementConn()

	svc, err := New(t.Context(), Options{
		Conn:  nc,
		Dir:   t.TempDir(),
		Group: "other",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if svc.EmbeddedServer() != nil {
		t.Error("expected no embedded server on a supplied conn")
	}
	if svc.Port() != 0 {
		t.Errorf("Port = %d, want 0 on a supplied conn", svc.Port())
	}
	if svc.ClientURL() != nc.ConnectedUrl() {
		t.Errorf("ClientURL = %q, want the supplied conn's URL %q", svc.ClientURL(), nc.ConnectedUrl())
	}

	var created api.CreateResponse
	mustRequest(t, nc, "other.create.server", api.CreateServerRequest{}, &created)
	if len(created.Servers) != 1 {
		t.Fatalf("created %d servers, want 1", len(created.Servers))
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if nc.IsClosed() {
		t.Error("Close closed a connection it was handed")
	}
}

// fakeProvider records the specs it is handed and hands back agents that
// forward nothing; it covers the agent path without a real agent. When agent is
// set every Start hands back that agent, so a test can drive the tester.shape.*
// endpoints against an agent with shaping. Otherwise each Start hands back a
// new agent without shaping.
type fakeProvider struct {
	mu     sync.Mutex
	specs  []agent.Spec
	closed bool
	err    error
	agent  *fakeAgent
}

func (f *fakeProvider) Start(_ context.Context, spec agent.Spec) (agent.Agent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}
	f.specs = append(f.specs, spec)
	port := 45000 + len(f.specs)

	if f.agent != nil {
		f.agent.mu.Lock()
		f.agent.port = port
		f.agent.mu.Unlock()
		return f.agent, nil
	}

	return &fakeAgent{port: port, noShaping: true}, nil
}

func (f *fakeProvider) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeProvider) snapshot() ([]agent.Spec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]agent.Spec(nil), f.specs...), f.closed
}

// fakeAgent records what each method was handed and answers with canned values.
// noShaping refuses every shaping method with agent.ErrUnsupported; err, when
// set, is returned by every shaping method otherwise.
type fakeAgent struct {
	mu        sync.Mutex
	port      int
	stopped   bool
	noShaping bool
	sets      []api.ShapingSet
	cleared   []string
	reported  []string
	setsCalls int
	err       error
	reports   []api.ShapingReport
	setIDs    []string
}

func (a *fakeAgent) Endpoint() backend.Endpoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	return backend.Endpoint{Host: "127.0.0.1", Port: a.port}
}

func (a *fakeAgent) Shape(_ context.Context, set api.ShapingSet) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.noShaping {
		return agent.ErrUnsupported
	}
	if a.err != nil {
		return a.err
	}
	a.sets = append(a.sets, set)
	return nil
}

func (a *fakeAgent) ClearShaping(_ context.Context, id string) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.noShaping {
		return nil, agent.ErrUnsupported
	}
	if a.err != nil {
		return nil, a.err
	}
	a.cleared = append(a.cleared, id)
	if id != "" {
		return []string{id}, nil
	}

	var ids []string
	for _, s := range a.sets {
		ids = append(ids, s.ID)
	}
	return ids, nil
}

func (a *fakeAgent) ShapingReport(_ context.Context, id string) ([]api.ShapingReport, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.noShaping {
		return nil, agent.ErrUnsupported
	}
	if a.err != nil {
		return nil, a.err
	}
	a.reported = append(a.reported, id)
	return a.reports, nil
}

func (a *fakeAgent) ShapingSets(context.Context) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.noShaping {
		return nil, agent.ErrUnsupported
	}
	if a.err != nil {
		return nil, a.err
	}
	a.setsCalls++
	return a.setIDs, nil
}

func (a *fakeAgent) Stop(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stopped = true
	return nil
}

var (
	_ agent.Provider = (*fakeProvider)(nil)
	_ agent.Agent    = (*fakeAgent)(nil)
)

// withAgent registers p as the "fake" agent and selects it.
func withAgent(p *fakeProvider) func(*Options) {
	return func(o *Options) {
		o.Agents = map[string]agent.Factory{"fake": func(context.Context, agent.Options) (agent.Provider, error) { return p, nil }}
		o.Agent = "fake"
	}
}

// TestTraceUsesAgent proves a traced create reaches the selected agent, that
// the port of its endpoint is published as the server's trace port, and that
// the spec describes the server being fronted.
func TestTraceUsesAgent(t *testing.T) {
	provider := &fakeProvider{}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Trace: true}, &created)

	specs, _ := provider.snapshot()
	if len(specs) != 1 {
		t.Fatalf("provider saw %d specs, want 1", len(specs))
	}

	spec := specs[0]
	if spec.InstanceID != created.ID {
		t.Errorf("InstanceID = %q, want %q", spec.InstanceID, created.ID)
	}
	if spec.ServerName != created.Servers[0].Name {
		t.Errorf("ServerName = %q, want %q", spec.ServerName, created.Servers[0].Name)
	}
	if !spec.Store {
		t.Error("Store = false, want true for a traced create")
	}

	if got := created.Servers[0].Ports["trace"]; got != 45001 {
		t.Errorf("trace port = %d, want the agent's 45001", got)
	}
}

// TestProxyUsesAgentWithoutStore proves a create with Proxy and no Trace starts
// the agent with Store false and still publishes the trace port.
func TestProxyUsesAgentWithoutStore(t *testing.T) {
	provider := &fakeProvider{}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Proxy: true}, &created)

	specs, _ := provider.snapshot()
	if len(specs) != 1 {
		t.Fatalf("provider saw %d specs, want 1", len(specs))
	}
	if specs[0].Store {
		t.Error("Store = true, want false for a proxy-only create")
	}
	if got := created.Servers[0].Ports["trace"]; got != 45001 {
		t.Errorf("trace port = %d, want the agent's 45001", got)
	}
}

// TestStatusListsShapingSets proves tester.status reports the ids of the sets the
// instance's agent holds, in the agent's order, and none for an instance without an
// agent.
func TestStatusListsShapingSets(t *testing.T) {
	fake := &fakeAgent{setIDs: []string{"lost-ack-30", "slow-pub"}}
	svc := startTestService(t, withAgent(&fakeProvider{agent: fake}))

	var shaped api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Proxy: true}, &shaped)
	var plain api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{}, &plain)

	var status api.StatusResponse
	mustRequest(t, svc.nc, "tester.status", api.StatusRequest{InstanceID: shaped.ID}, &status)
	if len(status.Instances) != 1 {
		t.Fatalf("status returned %d instances, want 1", len(status.Instances))
	}
	if got := status.Instances[0].ShapingSets; !reflect.DeepEqual(got, []string{"lost-ack-30", "slow-pub"}) {
		t.Errorf("shaping sets = %v, want [lost-ack-30 slow-pub]", got)
	}
	fake.mu.Lock()
	setsCalls := fake.setsCalls
	fake.mu.Unlock()
	if setsCalls != 1 {
		t.Errorf("agent was asked for its shaping sets %d times, want 1", setsCalls)
	}

	var plainStatus api.StatusResponse
	mustRequest(t, svc.nc, "tester.status", api.StatusRequest{InstanceID: plain.ID}, &plainStatus)
	if len(plainStatus.Instances) != 1 {
		t.Fatalf("status returned %d instances, want 1", len(plainStatus.Instances))
	}
	if got := plainStatus.Instances[0].ShapingSets; len(got) != 0 {
		t.Errorf("shaping sets = %v for an instance without an agent, want none", got)
	}
}

// TestTraceWithoutAgentIsRefused proves a service with no agent rejects a
// traced create rather than failing later or ignoring the request.
func TestTraceWithoutAgentIsRefused(t *testing.T) {
	svc := startTestService(t)

	payload, err := json.Marshal(api.CreateServerRequest{Trace: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	msg, err := svc.nc.Request("tester.create.server", payload, 10*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if got := msg.Header.Get("Nats-Service-Error-Code"); got != "014" {
		t.Errorf("error code = %q, want 014", got)
	}
}

// TestCloseClosesAgentProvider proves the agent provider is released with the
// service.
func TestCloseClosesAgentProvider(t *testing.T) {
	provider := &fakeProvider{}

	o := Options{Dir: t.TempDir()}
	withAgent(provider)(&o)

	svc, err := New(t.Context(), o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, closed := provider.snapshot(); !closed {
		t.Error("Close did not close the agent provider")
	}
}

// exampleShapingSet is the design's lost-ack rule file as a typed set.
func exampleShapingSet() api.ShapingSet {
	return api.ShapingSet{
		ID:             "lost-ack-30",
		ConnectionName: "^adr50-fast-lostack$",
		Rules: []api.ShapingRule{{
			ID: "drop-ack-30",
			Match: api.ShapingMatch{
				Direction: "from_server",
				Verb:      []string{"MSG"},
				Subject: &api.SubjectMatch{
					Grammar: "{prefix:rest}.{flow:int}.{gap}.{seq:int}.{op:int}.$FI",
					Where:   map[string]any{"seq": float64(30)},
				},
				Payload: &api.PayloadMatch{JSON: map[string]any{"type": "ack"}},
			},
			Action: api.ShapingAction{Kind: api.ShapingDrop},
			Limit:  1,
		}},
	}
}

// TestShapeEndpointsReachAgent proves set, clear and report reach the traced
// instance's agent with the request's arguments and return what it answers.
func TestShapeEndpointsReachAgent(t *testing.T) {
	fake := &fakeAgent{reports: []api.ShapingReport{{
		ID: "lost-ack-30",
		Firings: []api.ShapingFiring{{
			ConnectionUUID: "conn-1",
			ClientName:     "adr50-fast-lostack",
			FrameID:        "conn-1-7",
			Rule:           "drop-ack-30",
			Action:         api.ShapingDrop,
			Time:           time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC),
		}},
	}}}
	svc := startTestService(t, withAgent(&fakeProvider{agent: fake}))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Trace: true}, &created)

	set := exampleShapingSet()
	var setResp api.ShapeSetResponse
	mustRequest(t, svc.nc, "tester.shape.set", api.ShapeSetRequest{InstanceID: created.ID, Set: set}, &setResp)
	if setResp.Set != set.ID {
		t.Errorf("set response = %q, want %q", setResp.Set, set.ID)
	}
	fake.mu.Lock()
	sets := append([]api.ShapingSet(nil), fake.sets...)
	fake.mu.Unlock()
	if len(sets) != 1 {
		t.Fatalf("agent saw %d sets, want 1", len(sets))
	}
	if !reflect.DeepEqual(sets[0], set) {
		t.Errorf("agent got set %+v, want %+v", sets[0], set)
	}

	var report api.ShapeReportResponse
	mustRequest(t, svc.nc, "tester.shape.report", api.ShapeReportRequest{InstanceID: created.ID, Set: set.ID}, &report)
	if !reflect.DeepEqual(report.Sets, fake.reports) {
		t.Errorf("report = %+v, want %+v", report.Sets, fake.reports)
	}
	fake.mu.Lock()
	reported := append([]string(nil), fake.reported...)
	fake.mu.Unlock()
	if !reflect.DeepEqual(reported, []string{set.ID}) {
		t.Errorf("agent was asked to report %v, want [%s]", reported, set.ID)
	}

	var cleared api.ShapeClearResponse
	mustRequest(t, svc.nc, "tester.shape.clear", api.ShapeClearRequest{InstanceID: created.ID}, &cleared)
	if !reflect.DeepEqual(cleared.Cleared, []string{set.ID}) {
		t.Errorf("cleared = %v, want [%s]", cleared.Cleared, set.ID)
	}
	fake.mu.Lock()
	clearedArgs := append([]string(nil), fake.cleared...)
	fake.mu.Unlock()
	if !reflect.DeepEqual(clearedArgs, []string{""}) {
		t.Errorf("agent was asked to clear %q, want the empty set meaning all", clearedArgs)
	}
}

// TestShapeRefusals proves an unknown instance, an instance without an agent and
// an agent without shaping are each refused with their own code.
func TestShapeRefusals(t *testing.T) {
	svc := startTestService(t, withAgent(&fakeProvider{agent: &fakeAgent{}}))

	var untraced api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{}, &untraced)

	plain := startTestService(t, withAgent(&fakeProvider{}))
	var traced api.CreateResponse
	mustRequest(t, plain.nc, "tester.create.server", api.CreateServerRequest{Trace: true}, &traced)

	cases := []struct {
		name string
		svc  *Service
		id   string
		code string
	}{
		{"unknown instance", svc, "does-not-exist", "404"},
		{"untraced instance", svc, untraced.ID, "015"},
		{"agent without shaping", plain, traced.ID, "016"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set := exampleShapingSet()
			if got := errorCodeFor(t, tc.svc.nc, "tester.shape.set", api.ShapeSetRequest{InstanceID: tc.id, Set: set}); got != tc.code {
				t.Errorf("shape.set code = %q, want %s", got, tc.code)
			}
			if got := errorCodeFor(t, tc.svc.nc, "tester.shape.clear", api.ShapeClearRequest{InstanceID: tc.id}); got != tc.code {
				t.Errorf("shape.clear code = %q, want %s", got, tc.code)
			}
			if got := errorCodeFor(t, tc.svc.nc, "tester.shape.report", api.ShapeReportRequest{InstanceID: tc.id}); got != tc.code {
				t.Errorf("shape.report code = %q, want %s", got, tc.code)
			}
		})
	}
}

// TestShapeErrorReachesCaller proves the text of an agent's shaping error is
// returned to the caller, so a compile error naming the rule and field is
// readable at the client.
func TestShapeErrorReachesCaller(t *testing.T) {
	fake := &fakeAgent{err: errors.New("rule drop-ack-30: subject grammar: unknown type")}
	svc := startTestService(t, withAgent(&fakeProvider{agent: fake}))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Trace: true}, &created)

	payload, err := json.Marshal(api.ShapeSetRequest{InstanceID: created.ID, Set: exampleShapingSet()})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := svc.nc.Request("tester.shape.set", payload, 10*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if got := msg.Header.Get("Nats-Service-Error-Code"); got != "017" {
		t.Errorf("error code = %q, want 017", got)
	}
	if got := msg.Header.Get("Nats-Service-Error"); got != fake.err.Error() {
		t.Errorf("error = %q, want %q", got, fake.err.Error())
	}
}
