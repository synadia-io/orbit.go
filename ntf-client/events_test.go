package ntf

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/ntf/events"
)

// connectEvents connects a Manager to the test service and closes it when the
// test ends.
func connectEvents(t *testing.T, ctx context.Context) *Manager {
	t.Helper()

	m, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(m.Close)
	return m
}

// collectEvents reads the events of instanceID from from for wait, and returns
// them once the read ends with the deadline, so a test sees every event and
// nothing more.
func collectEvents(t *testing.T, m *Manager, instanceID string, from EventsFrom, wait time.Duration) []Received {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	var got []Received
	for r, err := range m.Events(ctx, instanceID, from) {
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err != nil {
			t.Fatalf("events: %v", err)
		}
		got = append(got, r)
	}
	return got
}

// eventConsumers counts the consumers on the event stream that read the events
// of instanceID.
func eventConsumers(t *testing.T, ctx context.Context, m *Manager, instanceID string) int {
	t.Helper()

	js, err := jetstream.New(m.nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	stream, err := js.Stream(ctx, events.StreamName(eventsGroup))
	if err != nil {
		t.Fatalf("event stream: %v", err)
	}

	filter := events.InstanceFilter(eventsGroup, instanceID)
	lister := stream.ListConsumers(ctx)
	n := 0
	for info := range lister.Info() {
		if info.Config.FilterSubject == filter || slices.Contains(info.Config.FilterSubjects, filter) {
			n++
		}
	}
	if lister.Err() != nil {
		t.Fatalf("list consumers: %v", lister.Err())
	}
	return n
}

// deleteEvent removes the message at stream sequence seq from the event stream.
func deleteEvent(ctx context.Context, js jetstream.JetStream, seq uint64) error {
	stream, err := js.Stream(ctx, events.StreamName(eventsGroup))
	if err != nil {
		return err
	}
	return stream.DeleteMsg(ctx, seq)
}

// describe renders an event as category.action, such as node.stopped.
func describe(e events.Event) string {
	return fmt.Sprintf("%s.%s", e.EventCategory(), e.EventAction())
}

// TestEventsFollowAfterStatus pins that following after the EventsSeq a status
// call returned sees exactly the events after it, in order, with their stream
// sequences and subjects.
func TestEventsFollowAfterStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m := connectEvents(t, ctx)
	inst, err := m.CreateServer(ctx, false, WithDescription(t.Name()))
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer inst.Destroy(t)

	status, err := m.InstanceStatus(ctx, inst.ID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.EventsSeq == 0 {
		t.Fatalf("status events_seq = 0, want the sequence of the create's events")
	}

	srv := inst.Servers[0]
	_, err = m.StopServer(ctx, srv.Name)
	if err != nil {
		t.Fatalf("stop server: %v", err)
	}
	_, err = m.StartServer(ctx, srv.Name)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}

	got := collectEvents(t, m, inst.ID, EventsAfter(status.EventsSeq), 2*time.Second)
	if len(got) != 2 {
		t.Fatalf("events after %d = %d, want 2 (stopped, started)", status.EventsSeq, len(got))
	}

	want := []events.NodeEventAction{events.NodeStopped, events.NodeStarted}
	last := status.EventsSeq
	for i, r := range got {
		node, ok := r.Event.(*events.NodeAction)
		if !ok {
			t.Fatalf("event %d = %T, want *events.NodeAction", i, r.Event)
		}
		if node.Action != want[i] {
			t.Errorf("event %d action = %q, want %q", i, node.Action, want[i])
		}
		if node.ServerName != srv.Name {
			t.Errorf("event %d server_name = %q, want %q", i, node.ServerName, srv.Name)
		}
		if r.Sequence <= last {
			t.Errorf("event %d sequence = %d, want above %d", i, r.Sequence, last)
		}
		last = r.Sequence

		subject := fmt.Sprintf("ntf.events.tester.%s.node.%s", inst.ID, want[i])
		if r.Subject != subject {
			t.Errorf("event %d subject = %q, want %q", i, r.Subject, subject)
		}
	}
}

// TestEventsReplayAfterDestroy pins that reading from the start after the
// instance is destroyed sees its whole history, in order.
func TestEventsReplayAfterDestroy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m := connectEvents(t, ctx)
	inst, err := m.CreateServer(ctx, false, WithDescription(t.Name()))
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	// The test destroys the instance itself; this removes it when the test
	// fails before that, and finds nothing otherwise.
	defer func() {
		_, err := m.Destroy(context.Background(), inst.ID)
		if err != nil && !errors.Is(err, ErrInstanceNotFound) {
			t.Errorf("cleanup destroy: %v", err)
		}
	}()

	srv := inst.Servers[0]
	_, err = m.StopServer(ctx, srv.Name)
	if err != nil {
		t.Fatalf("stop server: %v", err)
	}
	_, err = m.StartServer(ctx, srv.Name)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	_, err = m.Destroy(ctx, inst.ID)
	if err != nil {
		t.Fatalf("destroy: %v", err)
	}

	got := collectEvents(t, m, inst.ID, EventsFromStart(), 2*time.Second)

	var seen []string
	for _, r := range got {
		seen = append(seen, describe(r.Event))
	}
	want := []string{"instance.created", "node.ready", "node.stopped", "node.started", "instance.destroyed"}
	if !slices.Equal(seen, want) {
		t.Fatalf("events = %v, want %v", seen, want)
	}

	for i, r := range got {
		base := r.Event.EventBase()
		if base.InstanceID != inst.ID {
			t.Errorf("event %d instance_id = %q, want %q", i, base.InstanceID, inst.ID)
		}
		if base.SourceSeq != uint64(i+1) {
			t.Errorf("event %d source_seq = %d, want %d", i, base.SourceSeq, i+1)
		}
		if i > 0 && r.Sequence <= got[i-1].Sequence {
			t.Errorf("event %d sequence %d not above %d", i, r.Sequence, got[i-1].Sequence)
		}
	}

	created := got[0].Event.(*events.InstanceAction)
	if created.Kind != inst.Kind {
		t.Errorf("created kind = %q, want %q", created.Kind, inst.Kind)
	}
	destroyed := got[4].Event.(*events.InstanceAction)
	if destroyed.Reason != events.ReasonDestroy {
		t.Errorf("destroyed reason = %q, want %q", destroyed.Reason, events.ReasonDestroy)
	}
}

// TestEventsNewOnly pins that a new-only read sees only the events published
// after it started, and that ending its context deletes its consumer.
func TestEventsNewOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m := connectEvents(t, ctx)
	inst, err := m.CreateServer(ctx, false, WithDescription(t.Name()))
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer inst.Destroy(t)

	status, err := m.InstanceStatus(ctx, inst.ID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	type result struct {
		r   Received
		err error
	}
	results := make(chan result)
	rctx, rcancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for r, err := range m.Events(rctx, inst.ID, EventsNewOnly()) {
			select {
			case results <- result{r, err}:
			case <-rctx.Done():
				return
			}
		}
	}()

	// The read starts when its consumer exists; only then is an event new.
	for eventConsumers(t, ctx, m, inst.ID) == 0 {
		if ctx.Err() != nil {
			t.Fatalf("the new-only consumer was never created")
		}
		time.Sleep(20 * time.Millisecond)
	}

	srv := inst.Servers[0]
	_, err = m.StopServer(ctx, srv.Name)
	if err != nil {
		t.Fatalf("stop server: %v", err)
	}

	var first result
	select {
	case first = <-results:
	case <-ctx.Done():
		t.Fatalf("no event arrived after the stop")
	}
	if first.err != nil {
		t.Fatalf("events: %v", first.err)
	}
	if describe(first.r.Event) != "node.stopped" {
		t.Errorf("first new event = %s, want node.stopped", describe(first.r.Event))
	}
	if first.r.Sequence <= status.EventsSeq {
		t.Errorf("first new event sequence = %d, want above %d", first.r.Sequence, status.EventsSeq)
	}

	rcancel()
	<-done
	n := eventConsumers(t, ctx, m, inst.ID)
	if n != 0 {
		t.Errorf("consumers after the context ended = %d, want 0", n)
	}
}

// TestEventsNewOnlySurvivesReset pins that a new-only read loses no event when
// its consumer is recreated before it has read anything: the events stored
// between the loss and the recreation still arrive.
func TestEventsNewOnlySurvivesReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m := connectEvents(t, ctx)
	js, err := jetstream.New(m.nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	// The events are published straight to the stream, since a service
	// request takes longer than the read takes to recreate its consumer.
	id := fmt.Sprintf("reset-%d", time.Now().UnixNano())
	subject := fmt.Sprintf("ntf.events.tester.%s.future.thing", id)
	publish := func(eventID string) {
		t.Helper()

		data := fmt.Appendf(nil, `{"type":"io.synadia.ntf.event.v1.future_thing","id":%q,"timestamp":"2026-10-15T00:00:00Z","instance_id":%q,"source":"chaos","source_seq":1}`, eventID, id)
		ack, err := js.Publish(ctx, subject, data)
		if err != nil {
			t.Fatalf("publish %s: %v", eventID, err)
		}
		// The forged events would otherwise stay in the shared stream.
		t.Cleanup(func() {
			err := deleteEvent(context.Background(), js, ack.Sequence)
			if err != nil {
				t.Errorf("delete forged event %d: %v", ack.Sequence, err)
			}
		})
	}

	type result struct {
		r   Received
		err error
	}
	results := make(chan result, 10)
	rctx, rcancel := context.WithCancel(ctx)
	defer rcancel()
	go func() {
		for r, err := range m.Events(rctx, id, EventsNewOnly()) {
			select {
			case results <- result{r, err}:
			case <-rctx.Done():
				return
			}
		}
	}()

	names := waitEventConsumers(t, ctx, m, id, nil)
	for _, name := range names {
		err = js.DeleteConsumer(ctx, events.StreamName(eventsGroup), name)
		if err != nil {
			t.Fatalf("delete consumer %s: %v", name, err)
		}
	}

	// The first event is stored before the read has recreated its consumer,
	// the second after.
	publish("e1")
	waitEventConsumers(t, ctx, m, id, names)
	publish("e2")

	for _, want := range []string{"e1", "e2"} {
		var got result
		select {
		case got = <-results:
		case <-ctx.Done():
			t.Fatalf("event %s never arrived", want)
		}
		if got.err != nil {
			t.Fatalf("events: %v", got.err)
		}
		if got.r.Event.EventBase().ID != want {
			t.Fatalf("event = %s, want %s", got.r.Event.EventBase().ID, want)
		}
	}
}

// waitEventConsumers waits until a consumer reads the events of instanceID
// whose name is not in old, and returns the names of those that do.
func waitEventConsumers(t *testing.T, ctx context.Context, m *Manager, instanceID string, old []string) []string {
	t.Helper()

	js, err := jetstream.New(m.nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	stream, err := js.Stream(ctx, events.StreamName(eventsGroup))
	if err != nil {
		t.Fatalf("event stream: %v", err)
	}

	filter := events.InstanceFilter(eventsGroup, instanceID)
	for {
		var names []string
		lister := stream.ListConsumers(ctx)
		for info := range lister.Info() {
			if info.Config.FilterSubject != filter || slices.Contains(old, info.Name) {
				continue
			}
			names = append(names, info.Name)
		}
		if lister.Err() != nil {
			t.Fatalf("list consumers: %v", lister.Err())
		}
		if len(names) > 0 {
			return names
		}

		select {
		case <-ctx.Done():
			t.Fatalf("no new consumer for %s", instanceID)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestEventsBreakDeletesConsumer pins that breaking out of the loop deletes the
// consumer the read created, rather than leaving it until its inactivity
// threshold.
func TestEventsBreakDeletesConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m := connectEvents(t, ctx)
	inst, err := m.CreateServer(ctx, false, WithDescription(t.Name()))
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer inst.Destroy(t)

	read := 0
	for _, err := range m.Events(ctx, inst.ID, EventsFromStart()) {
		if err != nil {
			t.Fatalf("events: %v", err)
		}
		read++
		n := eventConsumers(t, ctx, m, inst.ID)
		if n != 1 {
			t.Fatalf("consumers while reading = %d, want 1", n)
		}
		break
	}
	if read != 1 {
		t.Fatalf("read %d events, want 1", read)
	}

	n := eventConsumers(t, ctx, m, inst.ID)
	if n != 0 {
		t.Errorf("consumers after break = %d, want 0", n)
	}
}

// TestEventsUnknownType pins that an event of a type this client does not know
// arrives as *events.UnknownEvent with its common fields and raw message, not
// as an error.
func TestEventsUnknownType(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m := connectEvents(t, ctx)
	js, err := jetstream.New(m.nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	// No instance is needed: the stream keeps any event published for an id.
	id := fmt.Sprintf("unknown-%d", time.Now().UnixNano())
	const eventType = "io.synadia.ntf.event.v1.future_thing"
	data := fmt.Appendf(nil, `{"type":%q,"id":"e1","timestamp":"2026-10-15T00:00:00Z","instance_id":%q,"source":"chaos","source_seq":1,"extra":true}`, eventType, id)
	subject := fmt.Sprintf("ntf.events.tester.%s.future.thing", id)
	ack, err := js.Publish(ctx, subject, data)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	// The forged event would otherwise stay in the shared stream.
	defer func() {
		err := deleteEvent(ctx, js, ack.Sequence)
		if err != nil {
			t.Errorf("delete forged event %d: %v", ack.Sequence, err)
		}
	}()

	got := collectEvents(t, m, id, EventsFromStart(), time.Second)
	if len(got) != 1 {
		t.Fatalf("events = %d, want 1", len(got))
	}

	unknown, ok := got[0].Event.(*events.UnknownEvent)
	if !ok {
		t.Fatalf("event = %T, want *events.UnknownEvent", got[0].Event)
	}
	if unknown.Type != eventType {
		t.Errorf("type = %q, want %q", unknown.Type, eventType)
	}
	if unknown.InstanceID != id {
		t.Errorf("instance_id = %q, want %q", unknown.InstanceID, id)
	}
	if string(unknown.Raw) != string(data) {
		t.Errorf("raw = %s, want %s", unknown.Raw, data)
	}
	if got[0].Sequence != ack.Sequence {
		t.Errorf("sequence = %d, want %d", got[0].Sequence, ack.Sequence)
	}
	if got[0].Subject != subject {
		t.Errorf("subject = %q, want %q", got[0].Subject, subject)
	}
}
