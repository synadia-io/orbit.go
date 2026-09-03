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
	"os"
	"path/filepath"
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
type fakeCapturer struct {
	mu       sync.Mutex
	requests []CaptureRequest
	closed   bool
	err      error
}

func (f *fakeCapturer) Capture(_ context.Context, req CaptureRequest) (CaptureProxy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}
	f.requests = append(f.requests, req)

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
