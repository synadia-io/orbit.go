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
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/backend"
)

// fakeBackend runs nothing. It records the options its factory was handed and
// whether it was closed, and declares the capabilities it was built with.
type fakeBackend struct {
	mu     sync.Mutex
	opts   backend.Options
	closed bool

	// capabilities is what Capabilities returns. It is set before the backend
	// is registered and never changed.
	capabilities []backend.Capability
}

// factory returns a backend.Factory that records its options on b and hands b
// back.
func (b *fakeBackend) factory() backend.Factory {
	return func(_ context.Context, opts backend.Options) (backend.Backend, error) {
		b.mu.Lock()
		b.opts = opts
		b.mu.Unlock()

		return b, nil
	}
}

func (b *fakeBackend) Capabilities() []backend.Capability { return b.capabilities }
func (b *fakeBackend) DefaultVersion() string             { return "0.0.0" }

func (b *fakeBackend) Placement(context.Context, string) (backend.Placement, error) {
	return nil, backend.ErrUnsupported
}

func (b *fakeBackend) Provision(context.Context, *backend.InstancePlan) ([]backend.Node, error) {
	return nil, backend.ErrUnsupported
}

func (b *fakeBackend) Teardown(context.Context, string) error { return nil }

func (b *fakeBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true
	return nil
}

var _ backend.Backend = (*fakeBackend)(nil)

// TestRegistrySelectsBackend proves New uses the in-process backend by default,
// and the registered backend Options.Backend names, built with the service's
// directory, preserve setting and logger.
func TestRegistrySelectsBackend(t *testing.T) {
	svc := startTestService(t)

	_, ok := svc.backend.(*inProcessBackend)
	if !ok {
		t.Errorf("default backend is %T, want the in-process backend", svc.backend)
	}

	fake := &fakeBackend{}
	svc = startTestService(t, func(o *Options) {
		o.Backends = map[string]backend.Factory{"fake": fake.factory()}
		o.Backend = "fake"
		o.Preserve = true
	})

	if svc.backend != fake {
		t.Fatalf("backend is %T, want the fake backend", svc.backend)
	}

	fake.mu.Lock()
	opts := fake.opts
	fake.mu.Unlock()

	if opts.Dir != svc.Dir() || !opts.Preserve || opts.Logger == nil {
		t.Errorf("factory options = %+v, want dir %s, preserve and a logger", opts, svc.Dir())
	}
}

// TestRegistryRejectsBadNames proves New fails on an empty or duplicate backend
// name with ErrBackendName, and on an unknown one with ErrUnknownBackend naming
// the registered backends.
func TestRegistryRejectsBadNames(t *testing.T) {
	fake := &fakeBackend{}

	tests := []struct {
		name     string
		backends map[string]backend.Factory
		backend  string
		want     error
		// message is text the error must contain, empty for none.
		message string
	}{
		{name: "empty name", backends: map[string]backend.Factory{"": fake.factory()}, want: ErrBackendName},
		{name: "inprocess", backends: map[string]backend.Factory{"inprocess": fake.factory()}, want: ErrBackendName},
		{name: "unknown", backends: map[string]backend.Factory{"fake": fake.factory()}, backend: "nope", want: ErrUnknownBackend, message: `"nope", registered: fake, inprocess`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, err := New(t.Context(), Options{Dir: t.TempDir(), Backends: tt.backends, Backend: tt.backend})
			if err == nil {
				svc.Close()
				t.Fatal("New succeeded, want an error")
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("error %v, want %v", err, tt.want)
			}
			if !strings.Contains(err.Error(), tt.message) {
				t.Errorf("error %q does not contain %q", err, tt.message)
			}
		})
	}
}

// TestCloseClosesBackend proves Service.Close closes the backend.
func TestCloseClosesBackend(t *testing.T) {
	fake := &fakeBackend{}

	svc, err := New(t.Context(), Options{Dir: t.TempDir(), Backends: map[string]backend.Factory{"fake": fake.factory()}, Backend: "fake"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = svc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	fake.mu.Lock()
	closed := fake.closed
	fake.mu.Unlock()

	if !closed {
		t.Error("Close did not close the backend")
	}
}

// TestStatusReportsBackendCapabilities proves status reports the name the
// selected backend was registered under and the capabilities it declares, in
// its order. The fake backend cannot create an instance, so the test registers
// one with no servers the way create does before placement.
func TestStatusReportsBackendCapabilities(t *testing.T) {
	fake := &fakeBackend{capabilities: []backend.Capability{backend.CapabilityVersionChange, backend.CapabilityKill}}
	svc := startTestService(t, func(o *Options) {
		o.Backends = map[string]backend.Factory{"fake": fake.factory()}
		o.Backend = "fake"
	})

	inst := svc.newInstance("server", "")

	var status api.StatusResponse
	mustRequest(t, svc.nc, "tester.status", api.StatusRequest{InstanceID: inst.ID}, &status)
	if len(status.Instances) != 1 {
		t.Fatalf("status returned %d instances, want 1", len(status.Instances))
	}

	got := status.Instances[0]
	if got.Backend != "fake" {
		t.Errorf("backend = %q, want fake", got.Backend)
	}
	want := []string{"version-change", "kill"}
	if !reflect.DeepEqual(got.Capabilities, want) {
		t.Errorf("capabilities = %v, want %v", got.Capabilities, want)
	}
}
