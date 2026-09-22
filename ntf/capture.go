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

	"github.com/synadia-io/orbit.go/ntf/api"
)

// NewCapturerFunc builds the trace capture implementation for a Service. It is
// called once at the end of New, with the Service already usable, so an
// implementation can reach the management connection, the embedded server and the
// instance directory. Returning an error fails New.
//
// Building a Capturer should stay cheap: doing the expensive setup on the first
// Capture call keeps services that never capture from paying for it.
type NewCapturerFunc func(context.Context, *Service) (Capturer, error)

// Capturer fronts managed servers with capture proxies that record the NATS
// traffic reaching them.
type Capturer interface {
	// Capture starts a proxy in front of one managed server's client port. It is
	// called for a single server created with Trace or Proxy, and for the first
	// node only of such a cluster or super-cluster. req.Store says whether to
	// keep captures. The caller stops the returned proxy on teardown.
	Capture(ctx context.Context, req CaptureRequest) (CaptureProxy, error)

	// Close releases the capturer. Called by Service.Close after instances have
	// been torn down and before the embedded server is shut down, so the
	// management connection is still usable.
	Close() error
}

// CaptureRequest describes the managed server a capture proxy is to front.
type CaptureRequest struct {
	// InstanceID is the instance the server belongs to.
	InstanceID string
	// ServerName is the managed server's name, recorded in the capture.
	ServerName string
	// Backend is the server's client address, as "127.0.0.1:4222".
	Backend string
	// ListenHost is the host the proxy listens on, taken from the node's
	// client_advertise. It must name a local interface. Empty binds 0.0.0.0.
	ListenHost string
	// TmpDir holds in-progress captures. The service creates it under the
	// instance directory and removes it with the instance, unless the service
	// was created with Preserve. Every traced node of an instance shares it.
	TmpDir string
	// Store is true when the create request asked for Trace and false when it
	// asked for Proxy alone. The capturer keeps captures only when Store is
	// true; when it is false the proxy still forwards and shapes every
	// connection.
	Store bool
}

// CaptureProxy is a running capture proxy fronting one managed server.
type CaptureProxy interface {
	// Port is the port the proxy listens on, which clients reach instead of the
	// managed server's own client port.
	Port() int
	// Stop closes live connections and waits for their captures to flush.
	Stop()
}

// Shaper is implemented by a CaptureProxy that can drop, stall, throttle or
// disconnect the frames passing through it. The tester.shape.* endpoints
// type-assert it on the traced instance's proxy and refuse the request when the
// proxy does not implement it.
type Shaper interface {
	// Shape adds a set to the proxy, replacing one with the same id and resetting
	// its counters. It compiles the set's rules and returns an error naming the
	// rule and field it could not compile; the error text reaches the caller.
	Shape(set api.ShapingSet) error
	// ClearShaping removes the named set, or every set when set is empty, and
	// returns the ids removed.
	ClearShaping(set string) ([]string, error)
	// ShapingReport returns the firings of the named set, or of every set when
	// set is empty.
	ShapingReport(set string) ([]api.ShapingReport, error)
}
