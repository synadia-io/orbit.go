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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/events"
)

// storedEvent is an event read back from an events stream.
type storedEvent struct {
	seq     uint64
	subject string
	event   events.Event
}

// eventsStream returns the events stream of group on the server nc reaches.
func eventsStream(t *testing.T, nc *nats.Conn, group string) jetstream.Stream {
	t.Helper()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	stream, err := js.Stream(t.Context(), events.StreamName(group))
	if err != nil {
		t.Fatalf("stream %s: %v", events.StreamName(group), err)
	}

	return stream
}

// storedEvents returns every event the events stream of group holds for
// instance, in stream order, each parsed with ParseEvent.
func storedEvents(t *testing.T, nc *nats.Conn, group string, instance string) []storedEvent {
	t.Helper()

	stream := eventsStream(t, nc, group)
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}

	prefix := strings.TrimSuffix(events.InstanceFilter(group, instance), ">")

	var stored []storedEvent
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		msg, err := stream.GetMsg(t.Context(), seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			t.Fatalf("get message %d: %v", seq, err)
		}
		if !strings.HasPrefix(msg.Subject, prefix) {
			continue
		}

		e, err := events.ParseEvent(msg.Data)
		if err != nil {
			t.Fatalf("parse event %d: %v", seq, err)
		}
		stored = append(stored, storedEvent{seq: msg.Sequence, subject: msg.Subject, event: e})
	}

	return stored
}

// waitForEvents waits until the events stream of group holds at least n events
// for instance, and returns them.
func waitForEvents(t *testing.T, nc *nats.Conn, group string, instance string, n int) []storedEvent {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		stored := storedEvents(t, nc, group, instance)
		if len(stored) >= n {
			return stored
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream holds %d events for %s, want %d", len(stored), instance, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// actionsOf returns the category and action of each event, as category.action.
func actionsOf(stored []storedEvent) []string {
	out := make([]string, 0, len(stored))
	for _, s := range stored {
		out = append(out, string(s.event.EventCategory())+"."+s.event.EventAction())
	}
	return out
}

// checkServiceEvents checks that stored are the service's events of instance,
// on their subjects in group, with source_seq counting from 1, and with the
// actions want.
func checkServiceEvents(t *testing.T, stored []storedEvent, group string, instance string, want []string) {
	t.Helper()

	got := actionsOf(stored)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("events = %v, want %v", got, want)
	}

	for i, s := range stored {
		b := s.event.EventBase()
		if b.InstanceID != instance {
			t.Errorf("event %d instance = %q, want %q", i, b.InstanceID, instance)
		}
		if b.Source != events.SourceService {
			t.Errorf("event %d source = %q, want service", i, b.Source)
		}
		if b.SourceSeq != uint64(i+1) {
			t.Errorf("event %d source_seq = %d, want %d", i, b.SourceSeq, i+1)
		}
		if b.Type != s.event.EventType() {
			t.Errorf("event %d type = %q, want %q", i, b.Type, s.event.EventType())
		}
		if b.ID == "" {
			t.Errorf("event %d has no id", i)
		}
		if b.Timestamp.IsZero() || b.Timestamp.Location() != time.UTC {
			t.Errorf("event %d timestamp = %v, want a UTC time", i, b.Timestamp)
		}

		subject, err := events.Subject(group, s.event)
		if err != nil {
			t.Fatalf("subject of event %d: %v", i, err)
		}
		if s.subject != subject {
			t.Errorf("event %d subject = %q, want %q", i, s.subject, subject)
		}
	}
}

// TestNewCreatesEventsStream proves New creates the events stream of its group
// with the default limits, and with the limits Options sets.
func TestNewCreatesEventsStream(t *testing.T) {
	cases := []struct {
		name     string
		opts     func(*Options)
		maxAge   time.Duration
		maxBytes int64
	}{
		{"defaults", func(*Options) {}, 24 * time.Hour, 1 << 30},
		{"configured", func(o *Options) {
			o.EventsMaxAge = time.Hour
			o.EventsMaxBytes = 1 << 20
		}, time.Hour, 1 << 20},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := startTestService(t, tc.opts)

			info, err := eventsStream(t, svc.nc, DefaultGroup).Info(t.Context())
			if err != nil {
				t.Fatalf("stream info: %v", err)
			}

			cfg := info.Config
			if cfg.Name != "NTF_EVENTS_TESTER" {
				t.Errorf("name = %q, want NTF_EVENTS_TESTER", cfg.Name)
			}
			if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "ntf.events.tester.>" {
				t.Errorf("subjects = %v, want [ntf.events.tester.>]", cfg.Subjects)
			}
			if cfg.Storage != jetstream.FileStorage {
				t.Errorf("storage = %v, want file", cfg.Storage)
			}
			if cfg.Discard != jetstream.DiscardOld {
				t.Errorf("discard = %v, want old", cfg.Discard)
			}
			if cfg.MaxAge != tc.maxAge {
				t.Errorf("max age = %v, want %v", cfg.MaxAge, tc.maxAge)
			}
			if cfg.MaxBytes != tc.maxBytes {
				t.Errorf("max bytes = %d, want %d", cfg.MaxBytes, tc.maxBytes)
			}
		})
	}
}

// TestNewFailsWhenJetStreamCannotStart proves New returns an error promptly,
// rather than waiting out its ready timeout, when the embedded server cannot
// enable JetStream in its store directory.
func TestNewFailsWhenJetStreamCannotStart(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	err := os.WriteFile(file, []byte("x"), 0o600)
	if err != nil {
		t.Fatalf("write %s: %v", file, err)
	}

	start := time.Now()
	svc, err := New(t.Context(), Options{Dir: t.TempDir(), Embedded: EmbeddedOptions{StoreDir: file}})
	if err == nil {
		svc.Close()
		t.Fatal("New succeeded with a store directory that is a file")
	}
	t.Logf("New failed: %v", err)
	took := time.Since(start)
	if took > readyTimeout/2 {
		t.Errorf("New took %v to fail, want well under the %v ready timeout", took, readyTimeout)
	}
}

// TestNewRejectsBadGroup proves New refuses a group that cannot name an events
// stream, upper case included.
func TestNewRejectsBadGroup(t *testing.T) {
	for _, group := range []string{"Tester", "TESTER", "a.b", "a*", "a>", "a b"} {
		t.Run(group, func(t *testing.T) {
			svc, err := New(t.Context(), Options{Dir: t.TempDir(), Group: group})
			if err == nil {
				svc.Close()
				t.Fatalf("New accepted group %q", group)
			}
			if !errors.Is(err, events.ErrInvalidGroup) {
				t.Errorf("err = %v, want ErrInvalidGroup", err)
			}
		})
	}
}

// TestEventsRecordLifecycle proves a create, a node stop and start, an update,
// a reload and a destroy each store their events in order, on their subjects,
// with source_seq counting from 1.
func TestEventsRecordLifecycle(t *testing.T) {
	svc := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{}, &created)
	name := created.Servers[0].Name

	mustRequest(t, svc.nc, "tester.stop.server", api.StopServerRequest{Name: name}, nil)
	mustRequest(t, svc.nc, "tester.start.server", api.StartServerRequest{Name: name}, nil)
	mustRequest(t, svc.nc, "tester.update.server", api.UpdateServerRequest{Name: name}, nil)
	mustRequest(t, svc.nc, "tester.reload.server", api.ReloadServerRequest{Name: name}, nil)
	mustRequest(t, svc.nc, "tester.destroy", api.DestroyRequest{InstanceID: created.ID}, nil)

	stored := storedEvents(t, svc.nc, DefaultGroup, created.ID)
	checkServiceEvents(t, stored, DefaultGroup, created.ID, []string{
		"instance.created", "node.ready", "node.stopped", "node.started", "node.updated", "node.reloaded", "instance.destroyed",
	})

	first, ok := stored[0].event.(*events.InstanceAction)
	if !ok {
		t.Fatalf("first event is %T, want *events.InstanceAction", stored[0].event)
	}
	if first.Kind != "server" || first.Backend != inProcessBackendName {
		t.Errorf("created kind %q backend %q, want server %s", first.Kind, first.Backend, inProcessBackendName)
	}

	for _, s := range stored[1:6] {
		node, ok := s.event.(*events.NodeAction)
		if !ok {
			t.Fatalf("event %d is %T, want *events.NodeAction", s.seq, s.event)
		}
		if node.Node != name || node.ServerName != name {
			t.Errorf("%s node %q server %q, want %q", node.Action, node.Node, node.ServerName, name)
		}
	}

	last, ok := stored[6].event.(*events.InstanceAction)
	if !ok {
		t.Fatalf("last event is %T, want *events.InstanceAction", stored[6].event)
	}
	if last.Reason != events.ReasonDestroy {
		t.Errorf("destroyed reason = %q, want destroy", last.Reason)
	}
}

// TestEventsDestroyReasons proves a reset and a close each record destroyed
// with their own reason, the close while the stream is still up.
func TestEventsDestroyReasons(t *testing.T) {
	host := startTestService(t)
	nc := host.ManagementConn()

	svc, err := New(t.Context(), Options{Conn: nc, Dir: t.TempDir(), Group: "other"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var reset api.CreateResponse
	mustRequest(t, nc, "other.create.server", api.CreateServerRequest{}, &reset)
	err = svc.Reset()
	if err != nil {
		t.Fatalf("Reset: %v", err)
	}

	var closed api.CreateResponse
	mustRequest(t, nc, "other.create.server", api.CreateServerRequest{}, &closed)
	err = svc.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	for id, reason := range map[string]events.DestroyReason{reset.ID: events.ReasonReset, closed.ID: events.ReasonClose} {
		stored := storedEvents(t, nc, "other", id)
		checkServiceEvents(t, stored, "other", id, []string{"instance.created", "node.ready", "instance.destroyed"})

		destroyed := stored[2].event.(*events.InstanceAction)
		if destroyed.Reason != reason {
			t.Errorf("destroyed reason = %q, want %q", destroyed.Reason, reason)
		}
	}
}

// TestStatusReportsEventsSeq proves status reports, per instance, the stream
// sequence of the latest service event of that instance.
func TestStatusReportsEventsSeq(t *testing.T) {
	svc := startTestService(t)

	var first, second api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{}, &first)
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{}, &second)
	mustRequest(t, svc.nc, "tester.stop.server", api.StopServerRequest{Name: first.Servers[0].Name}, nil)

	for _, id := range []string{first.ID, second.ID} {
		stored := storedEvents(t, svc.nc, DefaultGroup, id)
		want := stored[len(stored)-1].seq

		var status api.StatusResponse
		mustRequest(t, svc.nc, "tester.status", api.StatusRequest{InstanceID: id}, &status)
		got := status.Instances[0].EventsSeq
		if got != want {
			t.Errorf("events_seq of %s = %d, want %d", id, got, want)
		}
	}
}

// TestFailedCreatePublishesNothing proves a create that fails after its nodes
// are provisioned stores no event.
func TestFailedCreatePublishesNothing(t *testing.T) {
	svc := startTestService(t, withAgent(&fakeProvider{err: errors.New("agent refused")}))

	if got := errorCodeFor(t, svc.nc, "tester.create.server", api.CreateServerRequest{Trace: true}); got != "012" {
		t.Fatalf("create answered %q, want 012", got)
	}

	info, err := eventsStream(t, svc.nc, DefaultGroup).Info(t.Context())
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Errorf("stream holds %d events after a failed create, want none", info.State.Msgs)
	}
}

// TestCreateRacedByResetPublishesNothing stalls a cluster create while its
// second node starts, resets the service meanwhile, and proves neither created
// nor destroyed is stored for the instance the reset tore down before it was
// announced.
func TestCreateRacedByResetPublishesNothing(t *testing.T) {
	svc := startTestService(t)

	stalled := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0

	beforeServerStart = func(*server.Options) {
		mu.Lock()
		calls++
		call := calls
		mu.Unlock()

		if call == 2 {
			close(stalled)
			<-release
		}
	}
	t.Cleanup(func() { beforeServerStart = nil })

	payload, err := json.Marshal(api.CreateClusterRequest{Servers: 3})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	replies := make(chan *nats.Msg, 1)
	go func() {
		msg, err := svc.nc.Request("tester.create.cluster", payload, 30*time.Second)
		if err != nil {
			t.Errorf("request: %v", err)
		}
		replies <- msg
	}()

	select {
	case <-stalled:
	case <-time.After(30 * time.Second):
		t.Fatal("the create never started its second node")
	}

	resets := make(chan error, 1)
	go func() { resets <- svc.Reset() }()

	b := svc.backend.(*inProcessBackend)
	torn := func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()

		for _, rec := range b.instances {
			if rec.torn {
				return true
			}
		}
		return false
	}

	deadline := time.Now().Add(30 * time.Second)
	for !torn() {
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("the reset never marked the instance torn")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(release)

	msg := <-replies
	err = <-resets
	if err != nil {
		t.Errorf("Reset: %v", err)
	}
	if msg == nil {
		t.FailNow()
	}
	code := msg.Header.Get("Nats-Service-Error-Code")
	if code != "404" {
		t.Errorf("create answered %q, want 404", code)
	}

	info, err := eventsStream(t, svc.nc, DefaultGroup).Info(t.Context())
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Errorf("stream holds %d events for a create the reset tore down, want none", info.State.Msgs)
	}
}

// TestStopStartInstanceRecordOnlyChangedNodes proves stop.instance and
// start.instance record a node only when the request changed it, so a repeat
// records nothing.
func TestStopStartInstanceRecordOnlyChangedNodes(t *testing.T) {
	svc := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 3}, &created)
	id := created.ID

	mustRequest(t, svc.nc, "tester.stop.server", api.StopServerRequest{Name: created.Servers[0].Name}, nil)
	mustRequest(t, svc.nc, "tester.stop.instance", api.StopInstanceRequest{InstanceID: id}, nil)
	mustRequest(t, svc.nc, "tester.stop.instance", api.StopInstanceRequest{InstanceID: id}, nil)
	mustRequest(t, svc.nc, "tester.start.instance", api.StartInstanceRequest{InstanceID: id}, nil)
	mustRequest(t, svc.nc, "tester.start.instance", api.StartInstanceRequest{InstanceID: id}, nil)

	stored := storedEvents(t, svc.nc, DefaultGroup, id)
	checkServiceEvents(t, stored, DefaultGroup, id, []string{
		"instance.created", "node.ready", "node.ready", "node.ready",
		"node.stopped", "node.stopped", "node.stopped",
		"node.started", "node.started", "node.started",
	})

	stoppedNodes := map[string]bool{}
	for _, s := range stored[4:7] {
		stoppedNodes[s.event.(*events.NodeAction).Node] = true
	}
	if len(stoppedNodes) != 3 {
		t.Errorf("stopped events name %d distinct nodes, want 3", len(stoppedNodes))
	}
	first := stored[4].event.(*events.NodeAction).Node
	if first != created.Servers[0].Name {
		t.Errorf("first stopped node = %q, want the one stop.server stopped, %q", first, created.Servers[0].Name)
	}
}

// TestAgentPublisherStoresEvents proves the agent is handed a publisher whose
// events land in the stream stamped as the agent's, with a source_seq of their
// own per instance.
func TestAgentPublisherStoresEvents(t *testing.T) {
	provider := &fakeProvider{}
	svc := startTestService(t, withAgent(provider))

	provider.mu.Lock()
	pub := provider.opts.Events
	provider.mu.Unlock()
	if pub == nil {
		t.Fatal("the agent was handed no publisher")
	}

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{Proxy: true}, &created)

	applied := &events.ShapingAction{Base: events.Base{InstanceID: created.ID}, Action: events.ShapingApplied, Set: "lost-ack-30"}
	fired := &events.ShapingFired{Base: events.Base{InstanceID: created.ID}, Set: "lost-ack-30", Rule: "drop-ack-30", RuleAction: "drop", ConnectionUUID: "conn-1", FrameID: "conn-1-7"}
	other := &events.ShapingAction{Base: events.Base{InstanceID: "other-instance"}, Action: events.ShapingCleared, Set: "lost-ack-30"}

	for _, e := range []events.Event{applied, fired, other} {
		err := pub.Publish(t.Context(), e)
		if err != nil {
			t.Fatalf("Publish %s: %v", e.EventType(), err)
		}
	}

	err := pub.Publish(t.Context(), &events.ShapingAction{Action: events.ShapingApplied, Set: "no-instance"})
	if !errors.Is(err, events.ErrInvalidSubject) {
		t.Errorf("Publish without an instance = %v, want ErrInvalidSubject", err)
	}

	stored := waitForEvents(t, svc.nc, DefaultGroup, created.ID, 4)
	got := actionsOf(stored)
	want := []string{"instance.created", "node.ready", "shaping.applied", "shaping.fired"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("events = %v, want %v", got, want)
	}

	for i, s := range stored[2:] {
		b := s.event.EventBase()
		if b.Source != events.SourceAgent {
			t.Errorf("agent event %d source = %q, want agent", i, b.Source)
		}
		if b.SourceSeq != uint64(i+1) {
			t.Errorf("agent event %d source_seq = %d, want %d", i, b.SourceSeq, i+1)
		}
		if b.ID == "" || b.Type != s.event.EventType() || b.Timestamp.Location() != time.UTC {
			t.Errorf("agent event %d is not stamped: %+v", i, b)
		}
	}

	f := stored[3].event.(*events.ShapingFired)
	if f.FrameID != "conn-1-7" || f.Rule != "drop-ack-30" {
		t.Errorf("fired event lost its fields: %+v", f)
	}

	others := waitForEvents(t, svc.nc, DefaultGroup, "other-instance", 1)
	seq := others[0].event.EventBase().SourceSeq
	if seq != 1 {
		t.Errorf("source_seq of another instance's first agent event = %d, want 1", seq)
	}
}

// TestEventsLiveOnlyWithoutJetStream proves a service on a caller's connection
// to a server without JetStream starts, publishes its events live, and reports
// no events_seq.
func TestEventsLiveOnlyWithoutJetStream(t *testing.T) {
	srv, err := server.NewServer(&server.Options{Port: server.RANDOM_PORT, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv.Start()
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	sub, err := nc.SubscribeSync(events.StreamSubject(DefaultGroup))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	svc, err := New(t.Context(), Options{Conn: nc, Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		err := svc.Close()
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	var created api.CreateResponse
	mustRequest(t, nc, "tester.create.server", api.CreateServerRequest{}, &created)

	var got []string
	for range 2 {
		msg, err := sub.NextMsg(10 * time.Second)
		if err != nil {
			t.Fatalf("next event: %v", err)
		}
		e, err := events.ParseEvent(msg.Data)
		if err != nil {
			t.Fatalf("parse event: %v", err)
		}
		if e.EventBase().InstanceID != created.ID {
			t.Errorf("event instance = %q, want %q", e.EventBase().InstanceID, created.ID)
		}
		got = append(got, string(e.EventCategory())+"."+e.EventAction())
	}
	if strings.Join(got, " ") != "instance.created node.ready" {
		t.Errorf("live events = %v, want [instance.created node.ready]", got)
	}

	msg, err := nc.Request("tester.status", nil, 10*time.Second)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if strings.Contains(string(msg.Data), "events_seq") {
		t.Errorf("status reports events_seq without a stream: %s", msg.Data)
	}
}
