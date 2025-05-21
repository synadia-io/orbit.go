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
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/partitionedconsumergroups"
)

func TestStatic(t *testing.T) {
	streamName := "test"
	cgName := "group"
	var c1, c2 int

	server := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, server)

	nc, err := nats.Connect(server.ClientURL())
	require_NoError(t, err)

	ctx := context.Background()
	js, err := jetstream.New(nc)
	require_NoError(t, err)

	// Create a stream

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"bar.*"},
		SubjectTransform: &jetstream.SubjectTransformConfig{
			Source:      "bar.*",
			Destination: "{{partition(2,1)}}.bar.{{wildcard(1)}}",
		},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("bar.%d", i), []byte("payload"))
		require_NoError(t, err)
	}

	config := jetstream.ConsumerConfig{
		MaxAckPending: 1,
		AckWait:       1 * time.Second,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	_, err = partitionedconsumergroups.CreateStatic(ctx, js, streamName, cgName, 2, "bar.*", []string{"m1", "m2"}, []partitionedconsumergroups.MemberMapping{})
	require_NoError(t, err)

	sc1 := func() {
		partitionedconsumergroups.StaticConsume(ctx, js, streamName, cgName, "m1", func(msg jetstream.Msg) {
			c1++
			msg.Ack()
		}, config)
	}

	sc2 := func() {
		partitionedconsumergroups.StaticConsume(ctx, js, streamName, cgName, "m2", func(msg jetstream.Msg) {
			c2++
			msg.Ack()
		}, config)
	}

	go sc1()
	go sc2()

	now := time.Now()
	for {
		if c1+c2 == 10 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Since(now) > 5*time.Second {
			t.Fatalf("timeout")
		}
	}

	err = partitionedconsumergroups.DeleteStatic(ctx, js, streamName, cgName)
	require_NoError(t, err)
}

func TestElastic(t *testing.T) {
	var streamName = "test"
	var cgName = "group"
	var c1, c2 int

	server := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, server)

	nc, err := nats.Connect(server.ClientURL())
	require_NoError(t, err)

	ctx := context.Background()
	js, err := jetstream.New(nc)
	require_NoError(t, err)

	// Create a stream

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"bar.*"},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("bar.%d", i), []byte("payload"))
		require_NoError(t, err)
	}

	config := jetstream.ConsumerConfig{
		MaxAckPending: 1,
		AckWait:       1 * time.Second,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	_, err = partitionedconsumergroups.CreateElastic(ctx, js, streamName, cgName, 2, "bar.*", []int{1}, -1, -1)
	require_NoError(t, err)

	ec1 := func() {
		partitionedconsumergroups.ElasticConsume(ctx, js, streamName, cgName, "m1", func(msg jetstream.Msg) {
			c1++
			msg.Ack()
		}, config)
	}

	ec2 := func() {
		partitionedconsumergroups.ElasticConsume(ctx, js, streamName, cgName, "m2", func(msg jetstream.Msg) {
			c2++
			msg.Ack()
		}, config)
	}

	go ec1()
	go ec2()

	_, err = partitionedconsumergroups.AddMembers(ctx, js, streamName, cgName, []string{"m1"})
	require_NoError(t, err)

	now := time.Now()
	for {
		if c1 == 10 && c2 == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Since(now) > 5*time.Second {
			t.Fatalf("timeout")
		}
	}
	require_Equal(t, c1 == 10 && c2 == 0, true)

	_, err = partitionedconsumergroups.AddMembers(ctx, js, streamName, cgName, []string{"m2"})
	require_NoError(t, err)

	// wait a little bit for m2 to be effectively added (deletion and re-creation of the consumers)
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("bar.%d", i), []byte("payload"))
		require_NoError(t, err)
	}

	now = time.Now()
	for {
		if c1 == 15 && c2 == 5 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Since(now) > 10*time.Second {
			println(c1, c2)
			t.Fatalf("timeout")
		}
	}

	_, err = partitionedconsumergroups.DeleteMembers(ctx, js, streamName, cgName, []string{"m1"})
	require_NoError(t, err)

	// wait a little bit for m1 to be effectively deleted (deletion and re-creation of the consumers)
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("bar.%d", i), []byte("payload"))
		require_NoError(t, err)
	}

	now = time.Now()
	for {
		if c1 == 15 && c2 == 15 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Since(now) > 10*time.Second {
			println(c1, c2)
			t.Fatalf("timeout")
		}
	}

	err = partitionedconsumergroups.DeleteElastic(ctx, js, streamName, cgName)
	require_NoError(t, err)
}
