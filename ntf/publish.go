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
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	"github.com/synadia-io/orbit.go/ntf/events"
)

// eventAckTimeout limits how long the service waits for the stream to ack one
// of its events.
const eventAckTimeout = 5 * time.Second

// stampEvent fills the common fields of e other than instance_id: its type, a
// new id, the time now in UTC, source and seq as source_seq.
func stampEvent(e events.Event, source events.Source, seq uint64) {
	b := e.EventBase()
	b.Type = e.EventType()
	b.ID = nuid.Next()
	b.Timestamp = time.Now().UTC()
	b.Source = source
	b.SourceSeq = seq
}

// publishEvent publishes e as the service's next event of inst. With the stream
// in use it waits for the ack and moves the instance's events_seq up to the
// acked sequence; otherwise it publishes live only. A failure is logged, and
// leaves a gap in the instance's source_seq. The caller holds inst.events.
func (s *Service) publishEvent(inst *instance, e events.Event) {
	inst.sourceSeq++
	e.EventBase().InstanceID = inst.ID
	stampEvent(e, events.SourceService, inst.sourceSeq)

	subject, err := events.Subject(s.group, e)
	if err != nil {
		s.log.Warn("Could not publish event", "instance", inst.ID, "type", e.EventType(), "err", err)
		return
	}

	data, err := json.Marshal(e)
	if err != nil {
		s.log.Warn("Could not publish event", "instance", inst.ID, "subject", subject, "err", err)
		return
	}

	if s.js == nil {
		err = s.nc.Publish(subject, data)
		if err != nil {
			s.log.Warn("Could not publish event", "instance", inst.ID, "subject", subject, "err", err)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), eventAckTimeout)
	defer cancel()

	ack, err := s.js.Publish(ctx, subject, data)
	if err != nil {
		s.log.Warn("Could not store event", "instance", inst.ID, "subject", subject, "stream", s.stream, "err", err)
		return
	}

	s.mu.Lock()
	inst.eventsSeq = max(inst.eventsSeq, ack.Sequence)
	s.mu.Unlock()
}

// recordEvent publishes e as the service's next event of the instance
// instanceID, under its event lock, unless a destroy or reset has removed the
// instance. A handler calls it at the success point of a change it made.
func (s *Service) recordEvent(instanceID string, e events.Event) {
	s.mu.Lock()
	inst := s.instances[instanceID]
	s.mu.Unlock()

	if inst == nil {
		return
	}

	inst.events.Lock()
	defer inst.events.Unlock()

	// A destroy or reset removes the instance before it takes the event lock,
	// so an instance still registered here has not recorded destroyed yet.
	s.mu.Lock()
	_, alive := s.instances[instanceID]
	s.mu.Unlock()

	if !alive {
		return
	}

	s.publishEvent(inst, e)
}

// recordNodeEvent records action on the node ms, which status names
// serverName. The caller holds ms.cfgMu, so the event is ordered against the
// node's other changes.
func (s *Service) recordNodeEvent(ms *managedServer, action events.NodeEventAction, serverName string) {
	s.recordEvent(ms.instanceID, &events.NodeAction{Action: action, Node: ms.backendNode.Name(), ServerName: serverName})
}

// agentPublisher is the events.Publisher the service hands its agent in
// agent.Options. It stamps every event's common fields other than
// instance_id, with source agent and the next source_seq of the event's
// instance, and publishes it on the management connection without waiting for
// an ack, in one step under its own lock so source_seq follows publish order.
type agentPublisher struct {
	nc    *nats.Conn
	group string

	mu sync.Mutex
	// seqs holds the last source_seq of each instance.
	seqs map[string]uint64
}

func newAgentPublisher(nc *nats.Conn, group string) *agentPublisher {
	return &agentPublisher{nc: nc, group: group, seqs: map[string]uint64{}}
}

// Publish stamps e and publishes it on its subject. The caller fills
// instance_id and the fields of the event's type. An event that cannot be
// encoded or placed on a subject is refused and uses no source_seq; one that
// fails to publish leaves a gap.
func (p *agentPublisher) Publish(ctx context.Context, e events.Event) error {
	err := ctx.Err()
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	instanceID := e.EventBase().InstanceID
	seq := p.seqs[instanceID] + 1
	stampEvent(e, events.SourceAgent, seq)

	subject, err := events.Subject(p.group, e)
	if err != nil {
		return err
	}

	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("%w: %w", events.ErrInvalidEvent, err)
	}

	p.seqs[instanceID] = seq

	err = p.nc.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrEventPublish, err)
	}

	return nil
}

var _ events.Publisher = (*agentPublisher)(nil)
