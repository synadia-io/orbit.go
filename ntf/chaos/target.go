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

package chaos

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/events"
)

// SystemAccount is the name of the system account in the default template,
// the account Target.Conn connects to for the system API.
const SystemAccount = "$SYS"

// ConnectionNamePrefix starts the name of every connection a Target opens, so
// a kind that disconnects clients can leave the engine's own alone.
const ConnectionNamePrefix = "ntf-chaos"

// Target is what a kind acts on: one instance of an ntf service, reached
// through public paths only. The engine gives each worker a Target of its
// own, naming the run and the worker, over connections the run's workers
// share. Its methods are safe to call concurrently.
type Target interface {
	// InstanceID returns the id of the instance.
	InstanceID() string

	// RunID returns the id of the run.
	RunID() string

	// Worker returns the name of the worker.
	Worker() string

	// Status returns the instance's shape as tester.status reports it now:
	// its servers, links, toxics and the capabilities of its backend and
	// agent.
	Status(ctx context.Context) (*api.InstanceStatus, error)

	// Tester sends the tester.* request named by endpoint, such as
	// "stop.server" or "toxic.add", with req encoded as JSON, and decodes
	// the reply into resp, which may be nil. An error reply is returned as a
	// *TesterError.
	Tester(ctx context.Context, endpoint string, req any, resp any) error

	// Conn returns a connection to the server named server, as the account
	// named account, SystemAccount for the system account, using the run's
	// credentials for that account. It dials the server's own client port
	// rather than any proxy in front of it, so toxics never cut it, and names
	// it with ConnectionNamePrefix. The connection belongs to the Target and
	// is shared: a kind must not close or drain it.
	Conn(ctx context.Context, server string, account string) (*nats.Conn, error)

	// ServerID returns the server id of the server named server now. A
	// stopped and started server has a new id, so a kind looks it up on each
	// Apply rather than keeping it.
	ServerID(ctx context.Context, server string) (string, error)

	// Events returns the sink the engine publishes the run's events through.
	Events() events.Publisher
}

// TesterError is an error reply to a tester.* request: the micro error code
// and description the service answered with. It wraps ErrTester.
type TesterError struct {
	// Endpoint is the endpoint the request was sent to, such as
	// "stop.server".
	Endpoint string

	// Code is the error code, such as "003".
	Code string

	// Description is the error description.
	Description string
}

// Error returns the endpoint, code and description.
func (e *TesterError) Error() string {
	return fmt.Sprintf("%s: %s: %s %s", ErrTester, e.Endpoint, e.Code, e.Description)
}

// Unwrap returns ErrTester.
func (e *TesterError) Unwrap() error {
	return ErrTester
}
