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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// Type ids of the clock request and reply. They are not events: nothing
// publishes them to the events stream and ParseEvent does not know them, but
// Schema returns their schemas.
const (
	TypeClockRequest = "io.synadia.ntf.clock.v1.request"
	TypeClockReply   = "io.synadia.ntf.clock.v1.reply"
)

// ClockEndpoint is the name of the micro endpoint a participant answers clock
// requests on.
const ClockEndpoint = "clock"

// ClockSamples is how many requests ProbeClock sends.
const ClockSamples = 5

// clockProbeTimeout bounds ProbeClock when its context has no deadline.
const clockProbeTimeout = 5 * time.Second

// ClockRequest asks a participant for its clock.
//
// NTF Schema Type io.synadia.ntf.clock.v1.request
type ClockRequest struct {
	// Type is TypeClockRequest.
	Type string `json:"type"`
}

// ClockReply is a participant's answer to a ClockRequest.
//
// NTF Schema Type io.synadia.ntf.clock.v1.reply
type ClockReply struct {
	// Type is TypeClockReply.
	Type string `json:"type"`

	// Received is the participant's clock when the request arrived. It is
	// encoded in RFC 3339 with nanoseconds.
	Received time.Time `json:"received"`

	// Sent is the participant's clock when it sent the reply. It is encoded
	// in RFC 3339 with nanoseconds.
	Sent time.Time `json:"sent"`
}

// ClockHandler answers clock requests as a micro endpoint. A component joins
// the clock check by adding it to its own micro service as the endpoint named
// ClockEndpoint, under a group no other component or replica uses, and the
// caller of the check lists that endpoint's subject, here workload.clock:
//
//	err := svc.AddGroup("workload").AddEndpoint(events.ClockEndpoint, &events.ClockHandler{})
//
// A subject answered by several processes reaches only one of them per
// request, so the probe would mix their clocks into one result.
//
// It accepts a ClockRequest or an empty body, and refuses anything else with
// error code 400.
type ClockHandler struct {
	// Now is the clock the handler reports. Nil uses time.Now.
	Now func() time.Time
}

// Handle answers req with a ClockReply.
func (h *ClockHandler) Handle(req micro.Request) {
	now := h.Now
	if now == nil {
		now = time.Now
	}
	received := now().UTC()

	if len(req.Data()) > 0 {
		var creq ClockRequest
		err := json.Unmarshal(req.Data(), &creq)
		if err != nil {
			req.Error("400", "Invalid clock request", nil)
			return
		}
		if creq.Type != TypeClockRequest {
			req.Error("400", fmt.Sprintf("Invalid clock request type %q", creq.Type), nil)
			return
		}
	}

	reply := ClockReply{Type: TypeClockReply, Received: received}
	reply.Sent = now().UTC()
	req.RespondJSON(reply)
}

// ClockSample is a participant's clock measured against the local clock.
type ClockSample struct {
	// Offset is the participant's clock less the local clock, positive when
	// the participant is ahead.
	Offset time.Duration

	// RoundTrip is the time the request and reply spent in flight: the local
	// time from sending the request to reading the reply, less the time the
	// participant reports between receiving the request and sending the
	// reply.
	RoundTrip time.Duration

	// Bound is half of RoundTrip. The true offset lies within Bound of
	// Offset.
	Bound time.Duration
}

// ProbeClock sends ClockSamples clock requests to subject on nc, one after
// another, and returns the sample with the shortest round trip, the one whose
// offset is bounded most tightly.
//
// Each sample takes the local clock when the request is sent (t0) and when the
// reply is read (t3), and the reply's received (t1) and sent (t2) times. The
// offset is ((t1 - t0) + (t2 - t3)) / 2, the round trip (t3 - t0) - (t2 - t1),
// and the bound half the round trip. A reply whose sent time is before its
// received time, or further after it than the whole local round trip, does not
// count its time between the two, so its round trip is t3 - t0.
//
// A failed request, an error reply or a reply that is not a ClockReply fails
// the probe with ErrClockProbe. A ctx without a deadline is given one of 5
// seconds.
func ProbeClock(ctx context.Context, nc *nats.Conn, subject string) (ClockSample, error) {
	if subject == "" || strings.ContainsAny(subject, "*> \t\r\n") {
		return ClockSample{}, fmt.Errorf("%w: invalid subject %q", ErrClockProbe, subject)
	}

	_, ok := ctx.Deadline()
	if !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, clockProbeTimeout)
		defer cancel()
	}

	payload, err := json.Marshal(ClockRequest{Type: TypeClockRequest})
	if err != nil {
		return ClockSample{}, fmt.Errorf("%w: %w", ErrClockProbe, err)
	}

	var best ClockSample
	for i := range ClockSamples {
		sample, err := probeClockOnce(ctx, nc, subject, payload)
		if err != nil {
			return ClockSample{}, err
		}
		if i == 0 || sample.RoundTrip < best.RoundTrip {
			best = sample
		}
	}

	return best, nil
}

// probeClockOnce takes one sample of the clock answering on subject.
func probeClockOnce(ctx context.Context, nc *nats.Conn, subject string, payload []byte) (ClockSample, error) {
	t0 := time.Now()
	msg, err := nc.RequestWithContext(ctx, subject, payload)
	t3 := time.Now()
	if err != nil {
		return ClockSample{}, fmt.Errorf("%w: %s: %w", ErrClockProbe, subject, err)
	}

	e := msg.Header.Get(micro.ErrorHeader)
	if e != "" {
		return ClockSample{}, fmt.Errorf("%w: %s: %s %s", ErrClockProbe, subject, msg.Header.Get(micro.ErrorCodeHeader), e)
	}

	var reply ClockReply
	err = json.Unmarshal(msg.Data, &reply)
	if err != nil {
		return ClockSample{}, fmt.Errorf("%w: %s: %w", ErrClockProbe, subject, err)
	}
	if reply.Type != TypeClockReply {
		return ClockSample{}, fmt.Errorf("%w: %s: reply type %q", ErrClockProbe, subject, reply.Type)
	}
	if reply.Received.IsZero() || reply.Sent.IsZero() {
		return ClockSample{}, fmt.Errorf("%w: %s: reply without received or sent time", ErrClockProbe, subject)
	}

	// t0 and t3 carry a monotonic reading, so elapsed is immune to the local
	// clock stepping. The reply's times carry none, so the offsets below
	// compare wall clocks.
	elapsed := t3.Sub(t0)
	held := reply.Sent.Sub(reply.Received)
	if held < 0 || held > elapsed {
		held = 0
	}
	roundTrip := elapsed - held

	return ClockSample{
		Offset:    (reply.Received.Sub(t0) + reply.Sent.Sub(t3)) / 2,
		RoundTrip: roundTrip,
		Bound:     roundTrip / 2,
	}, nil
}
