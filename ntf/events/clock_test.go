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

package events

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// startClockServer starts an in-process nats-server and returns a connection
// to it, both closed when the test ends.
func startClockServer(t *testing.T) *nats.Conn {
	t.Helper()

	srv, err := server.NewServer(&server.Options{Port: server.RANDOM_PORT, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	srv.Start()
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}

	nc, err := nats.Connect("", nats.InProcessServer(srv))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	return nc
}

// addClockService registers a micro service in group answering clock requests
// with h, and returns the subject of its clock endpoint.
func addClockService(t *testing.T, nc *nats.Conn, group string, h micro.Handler) string {
	t.Helper()

	svc, err := micro.AddService(nc, micro.Config{Name: "clock-" + group, Version: "0.0.1"})
	if err != nil {
		t.Fatalf("add service: %v", err)
	}
	t.Cleanup(func() { svc.Stop() })

	err = svc.AddGroup(group).AddEndpoint(ClockEndpoint, h)
	if err != nil {
		t.Fatalf("add endpoint: %v", err)
	}

	return group + "." + ClockEndpoint
}

// shifted returns a clock running offset ahead of the local one.
func shifted(offset time.Duration) func() time.Time {
	return func() time.Time { return time.Now().Add(offset) }
}

// TestClockSchemas validates an example request and reply against their
// schemas, checks each schema's $id, title and type const, and checks the
// reply schema refuses a reply without its times and allows an extra field.
// The clock types stay out of the event registry, so ParseEvent treats them as
// unknown.
func TestClockSchemas(t *testing.T) {
	v := newSchemaValidator(t)

	now := time.Date(2026, 10, 15, 12, 30, 45, 123456789, time.UTC)
	clockExamples := map[string]any{
		TypeClockRequest: ClockRequest{Type: TypeClockRequest},
		TypeClockReply:   ClockReply{Type: TypeClockReply, Received: now, Sent: now.Add(time.Microsecond)},
	}

	for kind, example := range clockExamples {
		t.Run(kind, func(t *testing.T) {
			ok, errs := v.ValidateStruct(example, kind)
			if !ok {
				t.Fatalf("example does not validate: %v", errs)
			}

			schema, err := Schema(kind)
			if err != nil {
				t.Fatal(err)
			}
			var head struct {
				ID         string `json:"$id"`
				Title      string `json:"title"`
				Properties struct {
					Type struct {
						Const string `json:"const"`
					} `json:"type"`
				} `json:"properties"`
			}
			err = json.Unmarshal(schema, &head)
			if err != nil {
				t.Fatal(err)
			}

			name := strings.TrimPrefix(kind, "io.synadia.ntf.clock.v1.")
			wantID := "https://raw.githubusercontent.com/synadia-io/orbit.go/main/ntf/events/schemas/ntf/clock/v1/" + name + ".json"
			if head.ID != wantID {
				t.Errorf("$id is %q, want %q", head.ID, wantID)
			}
			if head.Title != kind {
				t.Errorf("title is %q, want %q", head.Title, kind)
			}
			if head.Properties.Type.Const != kind {
				t.Errorf("type const is %q, want %q", head.Properties.Type.Const, kind)
			}

			_, registered := registry[kind]
			if registered {
				t.Errorf("%s is in the event registry", kind)
			}
			data, err := json.Marshal(example)
			if err != nil {
				t.Fatal(err)
			}
			e, err := ParseEvent(data)
			if err != nil {
				t.Fatal(err)
			}
			_, unknown := e.(*UnknownEvent)
			if !unknown {
				t.Errorf("ParseEvent returned %T, want *UnknownEvent", e)
			}
		})
	}

	missing := map[string]any{"type": TypeClockReply, "received": "2026-10-15T12:30:45.123456789Z"}
	ok, _ := v.ValidateStruct(missing, TypeClockReply)
	if ok {
		t.Error("reply without sent validated")
	}

	extra := map[string]any{
		"type":        TypeClockReply,
		"received":    "2026-10-15T12:30:45.123456789Z",
		"sent":        "2026-10-15T12:30:45.123457789Z",
		"added_later": true,
	}
	ok, errs := v.ValidateStruct(extra, TypeClockReply)
	if !ok {
		t.Errorf("extra field refused: %v", errs)
	}
}

// TestProbeShiftedClock probes handlers whose clocks run ahead of, behind and
// level with the local one, and checks each reported offset is within its
// bound of the shift.
func TestProbeShiftedClock(t *testing.T) {
	nc := startClockServer(t)

	cases := map[string]time.Duration{
		"ahead":  3 * time.Second,
		"behind": -2 * time.Second,
		"level":  0,
	}

	for name, shift := range cases {
		t.Run(name, func(t *testing.T) {
			subject := addClockService(t, nc, name, &ClockHandler{Now: shifted(shift)})

			sample, err := ProbeClock(t.Context(), nc, subject)
			if err != nil {
				t.Fatalf("probe: %v", err)
			}

			if sample.RoundTrip <= 0 {
				t.Errorf("round trip %v, want above 0", sample.RoundTrip)
			}
			if sample.Bound != sample.RoundTrip/2 {
				t.Errorf("bound %v, want half the round trip %v", sample.Bound, sample.RoundTrip)
			}
			diff := (sample.Offset - shift).Abs()
			if diff > sample.Bound {
				t.Errorf("offset %v is %v from the shift %v, beyond the bound %v", sample.Offset, diff, shift, sample.Bound)
			}
		})
	}
}

// TestProbeFailures checks a probe fails with ErrClockProbe on a subject
// nothing answers, an invalid subject, an error reply and a reply that is not
// a clock reply.
func TestProbeFailures(t *testing.T) {
	nc := startClockServer(t)

	errorReply := addClockService(t, nc, "refuses", micro.HandlerFunc(func(req micro.Request) {
		req.Error("500", "no clock here", nil)
	}))
	wrongReply := addClockService(t, nc, "wrong", micro.HandlerFunc(func(req micro.Request) {
		req.Respond([]byte(`{"type":"io.synadia.ntf.event.v1.node_action"}`))
	}))

	cases := map[string]string{
		"no responders":   "nobody.clock",
		"wildcard":        "anybody.*",
		"empty":           "",
		"error reply":     errorReply,
		"not clock reply": wrongReply,
	}

	for name, subject := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ProbeClock(t.Context(), nc, subject)
			if !errors.Is(err, ErrClockProbe) {
				t.Errorf("got %v, want ErrClockProbe", err)
			}
		})
	}
}

// TestClockHandlerRequests checks the handler answers an empty body and a
// clock request, and refuses another type with code 400.
func TestClockHandlerRequests(t *testing.T) {
	nc := startClockServer(t)
	subject := addClockService(t, nc, "handler", &ClockHandler{})

	for name, body := range map[string]string{"empty": "", "request": `{"type":"io.synadia.ntf.clock.v1.request"}`} {
		t.Run(name, func(t *testing.T) {
			msg, err := nc.Request(subject, []byte(body), 5*time.Second)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			var reply ClockReply
			err = json.Unmarshal(msg.Data, &reply)
			if err != nil {
				t.Fatalf("reply: %v", err)
			}
			if reply.Type != TypeClockReply {
				t.Errorf("reply type %q, want %q", reply.Type, TypeClockReply)
			}
			if reply.Received.IsZero() || reply.Sent.Before(reply.Received) {
				t.Errorf("reply received %v, sent %v", reply.Received, reply.Sent)
			}
		})
	}

	msg, err := nc.Request(subject, []byte(`{"type":"io.synadia.ntf.clock.v2.request"}`), 5*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	code := msg.Header.Get(micro.ErrorCodeHeader)
	if code != "400" {
		t.Errorf("error code %q, want 400", code)
	}
}
