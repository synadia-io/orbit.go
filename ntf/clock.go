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
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go/micro"

	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/events"
)

// clockProbeTimeout bounds the probe of one extra clock subject, so a
// participant that does not answer cannot hold the reply past a client's
// request timeout.
const clockProbeTimeout = 3 * time.Second

// clock reports the clocks of the participants the service knows and of the
// extra subjects the request lists, against the service host's clock. The
// service is the reference. Every agent runs in the service's process, so it
// is reported as sharing the service's clock. Each extra subject is probed with
// events.ProbeClock, concurrently, and reported with its offset, round trip and
// bound, or with the error that stopped the probe.
func (s *Service) clock(req micro.Request) {
	creq := api.ClockRequest{}
	// Empty body is allowed and means no extra subjects.
	if len(req.Data()) > 0 {
		err := json.Unmarshal(req.Data(), &creq)
		if err != nil {
			req.Error("001", "Invalid request", nil)
			return
		}
	}

	s.mu.Lock()
	var agents []string
	for id, inst := range s.instances {
		if inst.agent != nil {
			agents = append(agents, id)
		}
	}
	s.mu.Unlock()
	slices.Sort(agents)

	resp := api.ClockResponse{Type: api.TypeClockResponse, Participants: []api.ClockParticipant{{Kind: api.ClockService, Reference: true}}}
	for _, id := range agents {
		resp.Participants = append(resp.Participants, api.ClockParticipant{Kind: api.ClockAgent, InstanceID: id, SharedClock: true})
	}

	extra := make([]api.ClockParticipant, len(creq.Extra))
	var wg sync.WaitGroup
	for i, subject := range creq.Extra {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), clockProbeTimeout)
			defer cancel()

			p := api.ClockParticipant{Kind: api.ClockExtra, Subject: subject}
			sample, err := events.ProbeClock(ctx, s.nc, subject)
			if err != nil {
				s.log.Warn("Could not probe a clock", "subject", subject, "err", err)
				p.Error = err.Error()
			} else {
				p.Offset = sample.Offset
				p.RoundTrip = sample.RoundTrip
				p.Bound = sample.Bound
			}
			extra[i] = p
		})
	}
	wg.Wait()

	resp.Participants = append(resp.Participants, extra...)

	req.RespondJSON(resp)
}
