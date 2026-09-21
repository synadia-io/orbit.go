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
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/synadia-io/orbit.go/ntf/api"
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

// fakeCapturer records the requests it is handed and hands back proxies that
// forward nothing; it covers the injection path without a real capture backend.
// When shaper is set every Capture hands back that proxy, so a test can drive the
// tester.shape.* endpoints against a proxy that implements Shaper.
type fakeCapturer struct {
	mu       sync.Mutex
	requests []CaptureRequest
	closed   bool
	err      error
	shaper   *fakeShapingProxy
}

func (f *fakeCapturer) Capture(_ context.Context, req CaptureRequest) (CaptureProxy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}
	f.requests = append(f.requests, req)

	if f.shaper != nil {
		f.shaper.port = 45000 + len(f.requests)
		return f.shaper, nil
	}

	return &fakeProxy{port: 45000 + len(f.requests)}, nil
}

func (f *fakeCapturer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeCapturer) snapshot() ([]CaptureRequest, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]CaptureRequest(nil), f.requests...), f.closed
}

type fakeProxy struct {
	port    int
	stopped bool
}

func (p *fakeProxy) Port() int { return p.port }
func (p *fakeProxy) Stop()     { p.stopped = true }

// fakeShapingProxy is a fakeProxy that implements Shaper, recording what each
// method was handed and answering with canned values. err, when set, is returned
// by every Shaper method.
type fakeShapingProxy struct {
	fakeProxy

	mu       sync.Mutex
	sets     []api.ShapingSet
	cleared  []string
	reported []string
	err      error
	reports  []api.ShapingReport
}

func (p *fakeShapingProxy) Shape(set api.ShapingSet) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.err != nil {
		return p.err
	}
	p.sets = append(p.sets, set)
	return nil
}

func (p *fakeShapingProxy) ClearShaping(set string) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.err != nil {
		return nil, p.err
	}
	p.cleared = append(p.cleared, set)
	if set != "" {
		return []string{set}, nil
	}

	var ids []string
	for _, s := range p.sets {
		ids = append(ids, s.ID)
	}
	return ids, nil
}

func (p *fakeShapingProxy) ShapingReport(set string) ([]api.ShapingReport, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.err != nil {
		return nil, p.err
	}
	p.reported = append(p.reported, set)
	return p.reports, nil
}

func withCapturer(c Capturer) func(*Options) {
	return func(o *Options) {
		o.NewCapturer = func(context.Context, *Service) (Capturer, error) { return c, nil }
	}
}

// TestTraceUsesCapturer proves a traced create reaches the injected Capturer, that
// the port it reports is published as the server's trace port, and that the
// request describes the server being fronted.
func TestTraceUsesCapturer(t *testing.T) {
	capturer := &fakeCapturer{}
	svc := startTestService(t, withCapturer(capturer))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Trace: true}, &created)

	reqs, _ := capturer.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("capturer saw %d requests, want 1", len(reqs))
	}

	req := reqs[0]
	if req.InstanceID != created.ID {
		t.Errorf("InstanceID = %q, want %q", req.InstanceID, created.ID)
	}
	if req.ServerName != created.Servers[0].Name {
		t.Errorf("ServerName = %q, want %q", req.ServerName, created.Servers[0].Name)
	}
	if want := filepath.Join(svc.Dir(), created.ID, "traces"); req.TmpDir != want {
		t.Errorf("TmpDir = %q, want %q", req.TmpDir, want)
	}
	if _, err := os.Stat(req.TmpDir); err != nil {
		t.Errorf("service did not create TmpDir: %v", err)
	}

	if got := created.Servers[0].Ports["trace"]; got != 45001 {
		t.Errorf("trace port = %d, want the capturer's 45001", got)
	}
}

// TestTraceWithoutCapturerIsRefused proves a service with no capturer rejects a
// traced create rather than failing later or ignoring the request.
func TestTraceWithoutCapturerIsRefused(t *testing.T) {
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

// TestCloseClosesCapturer proves the capturer is released with the service.
func TestCloseClosesCapturer(t *testing.T) {
	capturer := &fakeCapturer{}

	svc, err := New(t.Context(), Options{Dir: t.TempDir(), NewCapturer: func(context.Context, *Service) (Capturer, error) {
		return capturer, nil
	}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, closed := capturer.snapshot(); !closed {
		t.Error("Close did not close the capturer")
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

// TestShapeEndpointsReachShaper proves set, clear and report reach the traced
// instance's proxy with the request's arguments and return what it answers.
func TestShapeEndpointsReachShaper(t *testing.T) {
	proxy := &fakeShapingProxy{reports: []api.ShapingReport{{
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
	svc := startTestService(t, withCapturer(&fakeCapturer{shaper: proxy}))

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Trace: true}, &created)

	set := exampleShapingSet()
	var setResp api.ShapeSetResponse
	mustRequest(t, svc.nc, "tester.shape.set", api.ShapeSetRequest{InstanceID: created.ID, Set: set}, &setResp)
	if setResp.Set != set.ID {
		t.Errorf("set response = %q, want %q", setResp.Set, set.ID)
	}
	proxy.mu.Lock()
	sets := append([]api.ShapingSet(nil), proxy.sets...)
	proxy.mu.Unlock()
	if len(sets) != 1 {
		t.Fatalf("proxy saw %d sets, want 1", len(sets))
	}
	if !reflect.DeepEqual(sets[0], set) {
		t.Errorf("proxy got set %+v, want %+v", sets[0], set)
	}

	var report api.ShapeReportResponse
	mustRequest(t, svc.nc, "tester.shape.report", api.ShapeReportRequest{InstanceID: created.ID, Set: set.ID}, &report)
	if !reflect.DeepEqual(report.Sets, proxy.reports) {
		t.Errorf("report = %+v, want %+v", report.Sets, proxy.reports)
	}
	proxy.mu.Lock()
	reported := append([]string(nil), proxy.reported...)
	proxy.mu.Unlock()
	if !reflect.DeepEqual(reported, []string{set.ID}) {
		t.Errorf("proxy was asked to report %v, want [%s]", reported, set.ID)
	}

	var cleared api.ShapeClearResponse
	mustRequest(t, svc.nc, "tester.shape.clear", api.ShapeClearRequest{InstanceID: created.ID}, &cleared)
	if !reflect.DeepEqual(cleared.Cleared, []string{set.ID}) {
		t.Errorf("cleared = %v, want [%s]", cleared.Cleared, set.ID)
	}
	proxy.mu.Lock()
	clearedArgs := append([]string(nil), proxy.cleared...)
	proxy.mu.Unlock()
	if !reflect.DeepEqual(clearedArgs, []string{""}) {
		t.Errorf("proxy was asked to clear %q, want the empty set meaning all", clearedArgs)
	}
}

// TestShapeRefusals proves an unknown instance, an instance without a trace proxy
// and a proxy that does not implement Shaper are each refused with their own code.
func TestShapeRefusals(t *testing.T) {
	svc := startTestService(t, withCapturer(&fakeCapturer{shaper: &fakeShapingProxy{}}))

	var untraced api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{}, &untraced)

	plain := startTestService(t, withCapturer(&fakeCapturer{}))
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
		{"proxy without shaping", plain, traced.ID, "016"},
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

// TestShapeErrorReachesCaller proves the text of a Shaper error is returned to the
// caller, so a compile error naming the rule and field is readable at the client.
func TestShapeErrorReachesCaller(t *testing.T) {
	proxy := &fakeShapingProxy{err: errors.New("rule drop-ack-30: subject grammar: unknown type")}
	svc := startTestService(t, withCapturer(&fakeCapturer{shaper: proxy}))

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
	if got := msg.Header.Get("Nats-Service-Error"); got != proxy.err.Error() {
		t.Errorf("error = %q, want %q", got, proxy.err.Error())
	}
}
