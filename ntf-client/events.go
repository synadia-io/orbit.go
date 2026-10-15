package ntf

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/ntf/events"
)

const (
	// eventsGroup is the management API group of the service, which every
	// ntf-client call hardcodes.
	eventsGroup = "tester"
	// eventsResetAttempts caps how often the consumer behind Events tries to
	// recreate itself in one recovery. nats.go retries forever by default, so
	// a deleted stream would otherwise hang the loop.
	eventsResetAttempts = 3
)

// eventsStart is where Events starts reading.
type eventsStart int

const (
	eventsAfterSeq eventsStart = iota
	eventsFromStart
	eventsNewOnly
)

// EventsFrom says where Events starts reading an instance's events. Make one
// with EventsAfter, EventsFromStart or EventsNewOnly. The zero value reads from
// the start.
type EventsFrom struct {
	start eventsStart
	seq   uint64
}

// EventsAfter reads the events stored after stream sequence seq, normally the
// EventsSeq of the instance's status, for a follow with no gap from the state
// status showed. After 0 is the start, which suits an instance whose status has
// no EventsSeq yet.
func EventsAfter(seq uint64) EventsFrom {
	return EventsFrom{start: eventsAfterSeq, seq: seq}
}

// EventsFromStart reads every event the stream still holds for the instance,
// including after the instance was destroyed.
func EventsFromStart() EventsFrom {
	return EventsFrom{start: eventsFromStart}
}

// EventsNewOnly reads only the events published after reading starts.
func EventsNewOnly() EventsFrom {
	return EventsFrom{start: eventsNewOnly}
}

// Received is one event read by Events.
type Received struct {
	// Sequence is the event's stream sequence. Stream order is the order of
	// events across the service and its agents, and status's EventsSeq is a
	// sequence of the same stream. The sequence is not in the event itself. It
	// is zero for an event read without JetStream.
	Sequence uint64
	// Subject is the subject the event was published on.
	Subject string
	// Event is the event decoded with events.ParseEvent, such as
	// *events.NodeAction, or *events.UnknownEvent for a type this client does
	// not know.
	Event events.Event
}

// Events returns the events of the instance with the given ID, in stream order,
// from the point from names. Iteration starts reading and follows the stream
// until the loop breaks or ctx ends; each range over the result reads afresh.
//
// It reads through an ordered consumer on the service's event stream, filtered
// to the instance, so every event arrives once and in order. Breaking out of the
// loop or ending ctx stops the consumer and deletes it.
//
// An error that cannot be recovered from, including ctx ending, is yielded and
// ends the iteration. An event that cannot be decoded is yielded as an error
// wrapping events.ErrInvalidEvent, with its sequence and subject, and reading
// continues if the loop does.
//
// Without JetStream or an event stream on the management server, EventsAfter
// and EventsFromStart yield ErrNoEventStream at once, and EventsNewOnly reads
// from a plain subscription, whose events carry no sequence.
func (m *Manager) Events(ctx context.Context, instanceID string, from EventsFrom) iter.Seq2[Received, error] {
	return func(yield func(Received, error) bool) {
		if instanceID == "" || strings.ContainsAny(instanceID, ".*> \t\r\n") {
			yield(Received{}, fmt.Errorf("%w: invalid instance id %q", ErrEvents, instanceID))
			return
		}
		filter := events.InstanceFilter(eventsGroup, instanceID)

		js, err := jetstream.New(m.nc)
		if err != nil {
			yield(Received{}, fmt.Errorf("%w: %w", ErrEvents, err))
			return
		}

		cons, err := eventsConsumer(ctx, js, filter, from)
		if noEventStream(err) && from.start == eventsNewOnly {
			m.subscribeEvents(ctx, filter, yield)
			return
		}
		if noEventStream(err) {
			yield(Received{}, fmt.Errorf("%w: %w", ErrNoEventStream, err))
			return
		}
		if err != nil {
			yield(Received{}, fmt.Errorf("%w: %w", ErrEvents, err))
			return
		}

		followEvents(ctx, js, cons, yield)
	}
}

// eventsConsumer creates the ordered consumer that reads the events matching
// filter from the point from names.
func eventsConsumer(ctx context.Context, js jetstream.JetStream, filter string, from EventsFrom) (jetstream.Consumer, error) {
	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects:   []string{filter},
		DeliverPolicy:    jetstream.DeliverAllPolicy,
		MaxResetAttempts: eventsResetAttempts,
	}
	ctx, cancel := withDefaultTimeout(ctx, requestTimeout)
	defer cancel()

	switch from.start {
	case eventsAfterSeq:
		if from.seq > 0 {
			cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
			cfg.OptStartSeq = from.seq + 1
		}
	case eventsNewOnly:
		// New-only starts after the stream's last sequence rather than with
		// DeliverNew, since nats.go recreates a consumer that has read nothing
		// with its original policy, and DeliverNew would then skip the events
		// stored before the recreation.
		stream, err := js.Stream(ctx, events.StreamName(eventsGroup))
		if err != nil {
			return nil, err
		}
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = stream.CachedInfo().State.LastSeq + 1
	}

	return js.OrderedConsumer(ctx, events.StreamName(eventsGroup), cfg)
}

// noEventStream reports whether err means the management server has no
// JetStream or no event stream.
func noEventStream(err error) bool {
	return errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, jetstream.ErrJetStreamNotEnabled) ||
		errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount) ||
		errors.Is(err, jetstream.ErrStreamNotFound)
}

// followEvents yields the events cons delivers until yield returns false or an
// error ends the iteration, then stops cons and deletes it.
func followEvents(ctx context.Context, js jetstream.JetStream, cons jetstream.Consumer, yield func(Received, error) bool) {
	defer deleteEventsConsumer(ctx, js, cons)

	msgs, err := cons.Messages()
	if err != nil {
		yield(Received{}, fmt.Errorf("%w: %w", ErrEvents, err))
		return
	}
	defer msgs.Stop()

	for {
		msg, err := msgs.Next(jetstream.NextContext(ctx))
		if err != nil {
			yield(Received{}, fmt.Errorf("%w: %w", ErrEvents, err))
			return
		}
		meta, err := msg.Metadata()
		if err != nil {
			yield(Received{}, fmt.Errorf("%w: %w", ErrEvents, err))
			return
		}

		r := Received{Sequence: meta.Sequence.Stream, Subject: msg.Subject()}
		r.Event, err = events.ParseEvent(msg.Data())
		if err != nil {
			err = fmt.Errorf("%w: %d: %w", ErrEvents, r.Sequence, err)
		}
		if !yield(r, err) {
			return
		}
	}
}

// deleteEventsConsumer deletes the server-side consumer cons currently reads
// from, which stopping it leaves until its inactivity threshold. It runs when
// ctx has ended too, so it is bounded on its own.
func deleteEventsConsumer(ctx context.Context, js jetstream.JetStream, cons jetstream.Consumer) {
	info := cons.CachedInfo()
	if info == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), requestTimeout)
	defer cancel()

	// A consumer this misses still goes at its inactivity threshold, and the
	// loop has ended, so there is no one to report the error to.
	js.DeleteConsumer(ctx, info.Stream, info.Name)
}

// subscribeEvents yields the events published on filter from now on, through a
// plain subscription, until yield returns false or an error ends the iteration.
func (m *Manager) subscribeEvents(ctx context.Context, filter string, yield func(Received, error) bool) {
	sub, err := m.nc.SubscribeSync(filter)
	if err != nil {
		yield(Received{}, fmt.Errorf("%w: %w", ErrEvents, err))
		return
	}
	defer sub.Unsubscribe()

	// The subscription is in place once the server has answered a flush, so
	// every event published after that is read.
	fctx, cancel := withDefaultTimeout(ctx, requestTimeout)
	err = m.nc.FlushWithContext(fctx)
	cancel()
	if err != nil {
		yield(Received{}, fmt.Errorf("%w: %w", ErrEvents, err))
		return
	}

	for {
		msg, err := sub.NextMsgWithContext(ctx)
		if errors.Is(err, nats.ErrSlowConsumer) {
			// Events were dropped, and the subscription goes on.
			if !yield(Received{}, fmt.Errorf("%w: %w", ErrEvents, err)) {
				return
			}
			continue
		}
		if err != nil {
			yield(Received{}, fmt.Errorf("%w: %w", ErrEvents, err))
			return
		}

		r := Received{Subject: msg.Subject}
		r.Event, err = events.ParseEvent(msg.Data)
		if err != nil {
			err = fmt.Errorf("%w: %s: %w", ErrEvents, r.Subject, err)
		}
		if !yield(r, err) {
			return
		}
	}
}
