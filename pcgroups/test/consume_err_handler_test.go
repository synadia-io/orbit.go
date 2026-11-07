// Copyright 2025 Synadia Communications Inc.
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

package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/pcgroups"
)

func TestStaticConsumeErrHandlerConsumerDeleted(t *testing.T) {
	streamName := "test"
	cgName := "group"

	server := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, server)

	nc, err := nats.Connect(server.ClientURL())
	require_NoError(t, err)
	defer nc.Close()

	ctx := context.Background()
	js, err := jetstream.New(nc)
	require_NoError(t, err)

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"foo.*"},
		SubjectTransform: &jetstream.SubjectTransformConfig{
			Source:      "*.*",
			Destination: "{{partition(2,2)}}.{{wildcard(1)}}.{{wildcard(2)}}",
		},
	})
	require_NoError(t, err)

	_, err = pcgroups.CreateStatic(ctx, js, streamName, cgName, 2, []string{"foo.*"}, []string{"m1", "m2"}, []pcgroups.MemberMapping{})
	require_NoError(t, err)

	var received atomic.Uint32
	handlerErrs := make(chan error, 8)

	cc, err := pcgroups.StaticConsume(ctx, js, streamName, cgName, "m1", func(msg jetstream.Msg) {
		received.Add(1)
		msg.Ack()
	}, jetstream.ConsumerConfig{AckWait: time.Second, AckPolicy: jetstream.AckExplicitPolicy},
		pcgroups.WithConsumeErrHandler(func(err error) {
			select {
			case handlerErrs <- err:
			default:
			}
		}))
	require_NoError(t, err)
	defer cc.Stop()

	// make sure the member is consuming before the consumer is deleted
	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("foo.%d", i), []byte("payload"))
		require_NoError(t, err)
	}
	waitFor(t, 5*time.Second, func() bool { return received.Load() > 0 })

	require_NoError(t, js.DeleteConsumer(ctx, streamName, cgName+"-m1"))

	select {
	case err := <-handlerErrs:
		require_Equal(t, errors.Is(err, jetstream.ErrConsumerDeleted), true)
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for the error handler")
	}

	select {
	case err := <-cc.Done():
		if !errors.Is(err, jetstream.ErrConsumerDeleted) {
			t.Fatalf("expected Done() to report ErrConsumerDeleted, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for Done()")
	}
}

func TestElasticConsumeErrHandlerConsumerDeleted(t *testing.T) {
	streamName := "test"
	cgName := "group"

	server := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, server)

	nc, err := nats.Connect(server.ClientURL())
	require_NoError(t, err)
	defer nc.Close()

	ctx := context.Background()
	js, err := jetstream.New(nc)
	require_NoError(t, err)

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"foo.*"},
	})
	require_NoError(t, err)

	_, err = pcgroups.CreateElastic(ctx, js, streamName, cgName, 2, []pcgroups.PartitioningFilter{{Filter: "foo.*", PartitioningWildcards: []int{1}}}, -1, -1)
	require_NoError(t, err)

	var received atomic.Uint32
	handlerErrs := make(chan error, 8)

	cc, err := pcgroups.ElasticConsume(ctx, js, streamName, cgName, "m1", func(msg jetstream.Msg) {
		received.Add(1)
		msg.Ack()
	}, jetstream.ConsumerConfig{AckWait: time.Second, AckPolicy: jetstream.AckExplicitPolicy},
		pcgroups.WithConsumeErrHandler(func(err error) {
			select {
			case handlerErrs <- err:
			default:
			}
		}))
	require_NoError(t, err)
	defer cc.Stop()

	_, err = pcgroups.AddMembers(ctx, js, streamName, cgName, []string{"m1"})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("foo.%d", i), []byte("payload"))
		require_NoError(t, err)
	}
	waitFor(t, 6*time.Second, func() bool { return received.Load() == 10 })

	// the member's consumer lives on the consumer group's own stream
	require_NoError(t, js.DeleteConsumer(ctx, streamName+"-"+cgName, "m1"))

	select {
	case err := <-handlerErrs:
		require_Equal(t, errors.Is(err, jetstream.ErrConsumerDeleted), true)
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for the error handler")
	}

	// the member must not stay a zombie: it re-creates its consumer and keeps consuming
	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("foo.%d", i), []byte("payload"))
		require_NoError(t, err)
	}
	waitFor(t, 10*time.Second, func() bool { return received.Load() == 20 })
}

func TestConsumeNilErrHandler(t *testing.T) {
	_, err := pcgroups.StaticConsume(context.Background(), nil, "s", "g", "m", func(jetstream.Msg) {}, jetstream.ConsumerConfig{}, pcgroups.WithConsumeErrHandler(nil))
	if err == nil {
		t.Fatalf("expected an error for a nil error handler")
	}
	_, err = pcgroups.ElasticConsume(context.Background(), nil, "s", "g", "m", func(jetstream.Msg) {}, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy}, pcgroups.WithConsumeErrHandler(nil))
	if err == nil {
		t.Fatalf("expected an error for a nil error handler")
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
