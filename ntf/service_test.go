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
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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
// new agent without shaping. opts holds the Options its factory was handed
// when withAgent registered it. It declares capture unless noCapture is set,
// shaping only when the agents it hands back shape, and toxics when toxics is
// set.
//
// Start counts every call in starts, and returns the first of startErrs while
// any is left, then err when set. It records in bindable whether each port of
// the spec's links and client proxies could be bound as it began, and calls
// startHook, when set, with every spec it is handed, failed or not. The agents
// call frontGate, when set, as Front begins, record each Front in fronts and
// return the first of frontErrs while any is left. started lists the agents
// Start handed back, in order.
type fakeProvider struct {
	mu        sync.Mutex
	specs     []agent.Spec
	closed    bool
	err       error
	agent     *fakeAgent
	opts      agent.Options
	noCapture bool
	toxics    bool
	bindable  map[int]bool
	startHook func(agent.Spec)
	starts    int
	startErrs []error
	frontErrs []error
	frontGate func()
	fronts    []agent.Front
	started   []*fakeAgent
}

func (f *fakeProvider) Capabilities() []agent.Capability {
	var caps []agent.Capability
	if !f.noCapture {
		caps = append(caps, agent.CapabilityCapture)
	}
	if f.agent != nil && !f.agent.noShaping {
		caps = append(caps, agent.CapabilityShaping)
	}
	if f.toxics {
		caps = append(caps, agent.CapabilityToxics)
	}

	return caps
}

func (f *fakeProvider) Start(_ context.Context, spec agent.Spec) (agent.Agent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.starts++
	if f.bindable == nil {
		f.bindable = map[int]bool{}
	}
	for _, lk := range spec.Links {
		f.bindable[lk.Listen.Port] = portBindable(lk.Listen.Port)
	}
	for _, ep := range spec.ClientProxies {
		f.bindable[ep.Port] = portBindable(ep.Port)
	}
	if f.startHook != nil {
		f.startHook(spec)
	}

	if len(f.startErrs) > 0 {
		err := f.startErrs[0]
		f.startErrs = f.startErrs[1:]
		return nil, err
	}
	if f.err != nil {
		return nil, f.err
	}
	f.specs = append(f.specs, spec)
	port := 45000 + len(f.specs)

	if f.agent != nil {
		f.agent.mu.Lock()
		f.agent.port = port
		f.agent.provider = f
		f.agent.mu.Unlock()
		f.started = append(f.started, f.agent)
		return f.agent, nil
	}

	a := &fakeAgent{port: port, noShaping: true, provider: f}
	f.started = append(f.started, a)

	return a, nil
}

// front calls frontGate when set, then records a Front one of the provider's
// agents was handed and returns the error that Front fails with, if any.
func (f *fakeProvider) front(front agent.Front) error {
	f.mu.Lock()
	gate := f.frontGate
	f.mu.Unlock()

	if gate != nil {
		gate()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.fronts = append(f.fronts, front)
	if len(f.frontErrs) == 0 {
		return nil
	}

	err := f.frontErrs[0]
	f.frontErrs = f.frontErrs[1:]
	return err
}

// frontSnapshot returns the Fronts the provider's agents were handed and the
// agents Start handed back, in order.
func (f *fakeProvider) frontSnapshot() ([]agent.Front, []*fakeAgent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]agent.Front(nil), f.fronts...), append([]*fakeAgent(nil), f.started...)
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

// startCount returns the number of times Start was called, failed or not.
func (f *fakeProvider) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts
}

// fakeAgent records what each method was handed and answers with canned values.
// noShaping refuses every shaping method with agent.ErrUnsupported; err, when
// set, is returned by every shaping method otherwise. Front records itself with
// provider, the provider that started the agent, and the endpoint is the zero
// Endpoint until a Front succeeds.
type fakeAgent struct {
	mu        sync.Mutex
	port      int
	provider  *fakeProvider
	fronted   bool
	stopped   bool
	stops     int
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

	if !a.fronted {
		return backend.Endpoint{}
	}
	return backend.Endpoint{Host: "127.0.0.1", Port: a.port}
}

func (a *fakeAgent) Front(_ context.Context, front agent.Front) (backend.Endpoint, error) {
	a.mu.Lock()
	provider := a.provider
	a.mu.Unlock()

	err := provider.front(front)
	if err != nil {
		return backend.Endpoint{}, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.fronted = true
	return backend.Endpoint{Host: "127.0.0.1", Port: a.port}, nil
}

// isStopped reports whether Stop was called.
func (a *fakeAgent) isStopped() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stopped
}

// stopCount returns the number of times Stop was called.
func (a *fakeAgent) stopCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stops
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
	a.stops++
	return nil
}

var (
	_ agent.Provider   = (*fakeProvider)(nil)
	_ agent.Agent      = (*fakeAgent)(nil)
	_ agent.ClientEdge = (*fakeAgent)(nil)
)

// withAgent registers p as the "fake" agent and selects it.
func withAgent(p *fakeProvider) func(*Options) {
	return func(o *Options) {
		o.Agents = map[string]agent.Factory{"fake": func(_ context.Context, opts agent.Options) (agent.Provider, error) {
			p.mu.Lock()
			p.opts = opts
			p.mu.Unlock()
			return p, nil
		}}
		o.Agent = "fake"
	}
}

// TestTraceUsesAgent proves a traced create reaches the selected agent, that
// the port of its endpoint is published as the server's trace port, and that
// the spec describes the instance, and that the agent is asked to front the
// server.
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
	fronts, _ := provider.frontSnapshot()
	if len(fronts) != 1 {
		t.Fatalf("agent was fronted %d times, want 1", len(fronts))
	}
	if fronts[0].ServerName != created.Servers[0].Name {
		t.Errorf("ServerName = %q, want %q", fronts[0].ServerName, created.Servers[0].Name)
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

// TestAgentFrontsFirstNodeLiveClient proves a traced cluster create fronts its
// first node once, with that node's name and live client endpoint, and
// publishes the trace port on that node only.
func TestAgentFrontsFirstNodeLiveClient(t *testing.T) {
	provider := &fakeProvider{}
	svc := startTestService(t, withAgent(provider))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 3, Trace: true}, &created)

	fronts, started := provider.frontSnapshot()
	if len(started) != 1 {
		t.Fatalf("provider started %d agents, want 1", len(started))
	}
	if len(fronts) != 1 {
		t.Fatalf("agent was fronted %d times, want 1", len(fronts))
	}

	first := created.Servers[0]
	if fronts[0].ServerName != first.Name {
		t.Errorf("fronted %q, want the first node %q", fronts[0].ServerName, first.Name)
	}

	svc.mu.Lock()
	addrs := svc.instances[created.ID].Servers[0].backendNode.Addresses()
	svc.mu.Unlock()
	var live backend.Endpoint
	for _, a := range addrs {
		if a.Name == "client" {
			live = a.Internal
		}
	}
	if fronts[0].Client != live {
		t.Errorf("fronted client %+v, want the node's live client endpoint %+v", fronts[0].Client, live)
	}
	if fronts[0].Client.Port != first.Port {
		t.Errorf("fronted client port %d, want the node's client port %d", fronts[0].Client.Port, first.Port)
	}

	got := first.Ports["trace"]
	if got != 45001 {
		t.Errorf("trace port = %d, want the agent's 45001", got)
	}
	for _, srv := range created.Servers[1:] {
		_, ok := srv.Ports["trace"]
		if ok {
			t.Errorf("server %q carries a trace port, want only the first", srv.Name)
		}
	}
}

// TestAgentStoppedWhenProvisionFails takes the client port of every attempt's
// server before it binds, and proves each attempt starts its agent before its
// server, then stops it when Provision fails, both on the attempts that run
// again with fresh ports and on the final one, without fronting it.
func TestAgentStoppedWhenProvisionFails(t *testing.T) {
	provider := &fakeProvider{}
	svc := startTestService(t, withAgent(provider))

	var early atomic.Int32
	takePortHook(t, func(call int) bool {
		specs, _ := provider.snapshot()
		if len(specs) != call {
			early.Add(1)
		}
		return true
	})

	payload, err := json.Marshal(api.CreateServerRequest{Trace: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := svc.nc.Request("tester.create.server", payload, 30*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	code := msg.Header.Get("Nats-Service-Error-Code")
	if code != "003" {
		t.Fatalf("error code = %q, want 003", code)
	}

	n := early.Load()
	if n != 0 {
		t.Errorf("%d servers started before their attempt's agent", n)
	}
	fronts, started := provider.frontSnapshot()
	if len(started) != createAttempts {
		t.Fatalf("provider started %d agents, want one per attempt (%d)", len(started), createAttempts)
	}
	for i, a := range started {
		if !a.isStopped() {
			t.Errorf("agent of attempt %d was not stopped", i+1)
		}
	}
	if len(fronts) != 0 {
		t.Errorf("agent was fronted %d times, want none", len(fronts))
	}
}

// TestAgentStoppedOnReplan takes the client port of the first attempt's server
// and proves the agent that attempt started is stopped when the create plans
// again with fresh ports, while the agent of the attempt that succeeds keeps
// running and fronts the server.
func TestAgentStoppedOnReplan(t *testing.T) {
	provider := &fakeProvider{}
	svc := startTestService(t, withAgent(provider))
	takePortHook(t, func(call int) bool { return call == 1 })

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Trace: true}, &created)

	fronts, started := provider.frontSnapshot()
	if len(started) != 2 {
		t.Fatalf("provider started %d agents, want one per attempt (2)", len(started))
	}
	if !started[0].isStopped() {
		t.Error("agent of the failed attempt was not stopped")
	}
	if started[1].isStopped() {
		t.Error("agent of the successful attempt was stopped")
	}
	if len(fronts) != 1 {
		t.Errorf("agent was fronted %d times, want 1", len(fronts))
	}
	got := created.Servers[0].Ports["trace"]
	if got != 45002 {
		t.Errorf("trace port = %d, want the second agent's 45002", got)
	}
}

// TestCreateRetriesAfterAgentListen proves a create whose agent could not bind,
// in Start or in Front, runs again with fresh ports and succeeds, stopping the
// agent the failed attempt started.
func TestCreateRetriesAfterAgentListen(t *testing.T) {
	listenErr := fmt.Errorf("%w: port taken", agent.ErrListen)

	tests := []struct {
		name     string
		provider *fakeProvider
		// starts is the number of agents the create starts across its attempts.
		starts int
		// fronts is the number of times the agents are asked to front a server.
		fronts int
	}{
		{"start", &fakeProvider{startErrs: []error{listenErr}}, 1, 1},
		{"front", &fakeProvider{frontErrs: []error{listenErr}}, 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := startTestService(t, withAgent(tt.provider))

			var created api.CreateResponse
			mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Trace: true}, &created)

			if len(created.Servers) != 1 || !created.Servers[0].Running {
				t.Fatalf("create did not start a server: %+v", created.Servers)
			}
			if created.Servers[0].Ports["trace"] == 0 {
				t.Error("created server carries no trace port")
			}

			fronts, started := tt.provider.frontSnapshot()
			if len(started) != tt.starts {
				t.Fatalf("provider started %d agents, want %d", len(started), tt.starts)
			}
			if len(fronts) != tt.fronts {
				t.Errorf("agents were fronted %d times, want %d", len(fronts), tt.fronts)
			}
			for i, a := range started[:len(started)-1] {
				if !a.isStopped() {
					t.Errorf("agent %d of a failed attempt was not stopped", i+1)
				}
			}
			if started[len(started)-1].isStopped() {
				t.Error("agent of the successful attempt was stopped")
			}

			var list api.ListResponse
			mustRequest(t, svc.nc, "tester.list", nil, &list)
			if len(list.Instances) != 1 {
				t.Errorf("%d instances registered, want 1", len(list.Instances))
			}
		})
	}
}

// TestAgentStartFailureIsNotRetried proves a Start error that does not wrap
// agent.ErrListen answers 012 with its text at once, without running the create
// again, and leaves no instance registered.
func TestAgentStartFailureIsNotRetried(t *testing.T) {
	provider := &fakeProvider{err: errors.New("agent refused")}
	svc := startTestService(t, withAgent(provider))

	payload, err := json.Marshal(api.CreateServerRequest{Trace: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := svc.nc.Request("tester.create.server", payload, 30*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	code := msg.Header.Get("Nats-Service-Error-Code")
	if code != "012" {
		t.Fatalf("error code = %q, want 012", code)
	}
	desc := msg.Header.Get("Nats-Service-Error")
	if desc != "trace setup failed: agent refused" {
		t.Errorf("error = %q, want the agent's error", desc)
	}

	starts := provider.startCount()
	if starts != 1 {
		t.Errorf("Start was called %d times, want 1", starts)
	}

	var list api.ListResponse
	mustRequest(t, svc.nc, "tester.list", nil, &list)
	if len(list.Instances) != 0 {
		t.Errorf("failed create left %d instances registered", len(list.Instances))
	}
}

// TestAgentStoppedWhenFinalFrontFails proves the agent is stopped exactly once
// when the final attempt's Front fails and the create answers 012: at once for
// an error that does not wrap agent.ErrListen, and after every attempt for one
// that does.
func TestAgentStoppedWhenFinalFrontFails(t *testing.T) {
	listenErr := fmt.Errorf("%w: port taken", agent.ErrListen)

	tests := []struct {
		name      string
		frontErrs []error
		// attempts is the number of attempts the create runs.
		attempts int
	}{
		{"other error", []error{errors.New("agent refused")}, 1},
		{"listen on every attempt", []error{listenErr, listenErr, listenErr}, createAttempts},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &fakeProvider{frontErrs: tt.frontErrs}
			svc := startTestService(t, withAgent(provider))

			payload, err := json.Marshal(api.CreateServerRequest{Trace: true})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			msg, err := svc.nc.Request("tester.create.server", payload, 30*time.Second)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			code := msg.Header.Get("Nats-Service-Error-Code")
			if code != "012" {
				t.Fatalf("error code = %q, want 012", code)
			}

			fronts, started := provider.frontSnapshot()
			if len(started) != tt.attempts {
				t.Fatalf("provider started %d agents, want %d", len(started), tt.attempts)
			}
			if len(fronts) != tt.attempts {
				t.Errorf("agents were fronted %d times, want %d", len(fronts), tt.attempts)
			}
			for i, a := range started {
				stops := a.stopCount()
				if stops != 1 {
					t.Errorf("agent %d was stopped %d times, want 1", i+1, stops)
				}
			}
		})
	}
}

// TestAgentStoppedWhenCreateRacedByReset holds a traced create inside Front,
// resets the service meanwhile, and proves the create, finding its instance
// gone when it publishes, stops the agent exactly once and answers 404.
func TestAgentStoppedWhenCreateRacedByReset(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	provider := &fakeProvider{frontGate: func() {
		close(entered)
		<-release
	}}
	svc := startTestService(t, withAgent(provider))

	payload, err := json.Marshal(api.CreateServerRequest{Trace: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	codes := make(chan string, 1)
	go func() {
		msg, err := svc.nc.Request("tester.create.server", payload, 30*time.Second)
		if err != nil {
			t.Errorf("request: %v", err)
			codes <- ""
			return
		}
		codes <- msg.Header.Get("Nats-Service-Error-Code")
	}()

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		close(release)
		t.Fatal("the create never reached Front")
	}

	err = svc.Reset()
	close(release)
	if err != nil {
		t.Errorf("Reset: %v", err)
	}

	code := <-codes
	if code != "404" {
		t.Errorf("create answered %q, want 404", code)
	}

	_, started := provider.frontSnapshot()
	if len(started) != 1 {
		t.Fatalf("provider started %d agents, want 1", len(started))
	}
	stops := started[0].stopCount()
	if stops != 1 {
		t.Errorf("agent was stopped %d times, want 1", stops)
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

// TestTraceWithoutCaptureIsRefused proves every kind of create refuses trace
// and proxy with 018 when the selected agent does not declare capture, naming
// the agent and the capability, and never starts the agent.
func TestTraceWithoutCaptureIsRefused(t *testing.T) {
	provider := &fakeProvider{noCapture: true}
	svc := startTestService(t, withAgent(provider))

	tests := []struct {
		name    string
		subject string
		req     any
	}{
		{"server trace", "tester.create.server", api.CreateServerRequest{Trace: true}},
		{"server proxy", "tester.create.server", api.CreateServerRequest{Proxy: true}},
		{"cluster trace", "tester.create.cluster", api.CreateClusterRequest{Servers: 3, Trace: true}},
		{"super-cluster trace", "tester.create.super-cluster", api.CreateSuperClusterRequest{Clusters: 2, Servers: 3, Trace: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(tt.req)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			msg, err := svc.nc.Request(tt.subject, payload, 10*time.Second)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			code := msg.Header.Get("Nats-Service-Error-Code")
			if code != "018" {
				t.Errorf("error code = %q, want 018", code)
			}
			got := msg.Header.Get("Nats-Service-Error")
			if !strings.Contains(got, `"fake"`) || !strings.Contains(got, `"capture"`) {
				t.Errorf("error %q does not name the agent and the capability", got)
			}
		})
	}

	specs, _ := provider.snapshot()
	if len(specs) != 0 {
		t.Errorf("provider saw %d specs, want none", len(specs))
	}
}

// TestStatusReportsAgent proves status reports the name the selected agent was
// registered under and the capabilities it declares, in its order, and omits
// both when no agent is selected. The instances have no servers, as create
// registers them before placement.
func TestStatusReportsAgent(t *testing.T) {
	svc := startTestService(t, withAgent(&fakeProvider{agent: &fakeAgent{}}))
	inst := svc.newInstance("server", "")

	var status api.StatusResponse
	mustRequest(t, svc.nc, "tester.status", api.StatusRequest{InstanceID: inst.ID}, &status)
	if len(status.Instances) != 1 {
		t.Fatalf("status returned %d instances, want 1", len(status.Instances))
	}

	got := status.Instances[0]
	if got.Agent != "fake" {
		t.Errorf("agent = %q, want fake", got.Agent)
	}
	want := []string{"capture", "shaping"}
	if !reflect.DeepEqual(got.AgentCapabilities, want) {
		t.Errorf("agent capabilities = %v, want %v", got.AgentCapabilities, want)
	}

	plain := startTestService(t)
	plainInst := plain.newInstance("server", "")

	payload, err := json.Marshal(api.StatusRequest{InstanceID: plainInst.ID})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := plain.nc.Request("tester.status", payload, 10*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	var raw struct {
		Instances []map[string]json.RawMessage `json:"instances"`
	}
	err = json.Unmarshal(msg.Data, &raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(raw.Instances) != 1 {
		t.Fatalf("status returned %d instances, want 1", len(raw.Instances))
	}
	for _, key := range []string{"agent", "agent_capabilities"} {
		if _, ok := raw.Instances[0][key]; ok {
			t.Errorf("status of a service without an agent carries %q", key)
		}
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
