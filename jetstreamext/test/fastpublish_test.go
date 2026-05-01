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

package test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

func TestFastPublisher(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"test.>"},
			AllowBatchPublish: true,
		}
		stream, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// Create a fast publisher
		batch, err := jetstreamext.NewFastPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating fast publisher: %v", err)
		}

		// Add messages to the batch
		if _, err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message 1: %v", err)
		}

		fastAck, err := batch.AddMsg(&nats.Msg{
			Subject: "test.2",
			Data:    []byte("message 2"),
		})
		if err != nil {
			t.Fatalf("Unexpected error adding message 2: %v", err)
		}

		// Check ack
		if fastAck.BatchSequence != 2 {
			t.Fatalf("Expected fastAck.BatchSequence to be 2, got %d", fastAck.BatchSequence)
		}
		if fastAck.AckSequence != 0 {
			t.Fatalf("Expected fastAck.AckSequence to be 0, got %d", fastAck.AckSequence)
		}

		// Commit the batch
		ack, err := batch.Commit(ctx, "test.3", []byte("message 3"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		if ack == nil {
			t.Fatal("Expected non-nil BatchAck")
		}

		if ack.BatchSize != 3 {
			t.Fatalf("Expected BatchAck.BatchSize to be 3, got %d", ack.BatchSize)
		}

		if ack.BatchID == "" {
			t.Fatal("Expected non-empty BatchAck.BatchID")
		}

		if ack.Stream != "TEST" {
			t.Fatalf("Expected BatchAck.Stream to be TEST, got %s", ack.Stream)
		}

		// Verify batch is closed
		if !batch.IsClosed() {
			t.Fatal("Expected batch to be closed after commit")
		}

		// Verify we can't add more messages
		if _, err := batch.Add("test.4", []byte("message 4")); !errors.Is(err, jetstreamext.ErrBatchClosed) {
			t.Fatalf("Expected ErrBatchClosed adding to closed batch, got %v", err)
		}

		// verify we have 3 messages in the stream
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 3 {
			t.Fatalf("Expected 3 messages in the stream, got %d", info.State.Msgs)
		}
	})

	t.Run("with_flow_control", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"test.>"},
			AllowBatchPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// Create a fast publisher with custom flow control
		batch, err := jetstreamext.NewFastPublisher(js,
			jetstreamext.FastPublishFlowControl{
				Flow:               50,
				MaxOutstandingAcks: 3,
				AckTimeout:         3 * time.Second,
			},
		)
		if err != nil {
			t.Fatalf("Unexpected error creating fast publisher: %v", err)
		}

		// Add multiple messages
		for i := range 200 {
			if _, err := batch.Add("test.msg", []byte("data")); err != nil {
				t.Fatalf("Unexpected error adding message %d: %v", i, err)
			}
		}

		// Commit the batch
		ack, err := batch.Commit(ctx, "test.final", []byte("final"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		if ack.BatchSize != 201 {
			t.Fatalf("Expected BatchAck.BatchSize to be 201, got %d", ack.BatchSize)
		}
	})

	t.Run("with_continue_on_gap", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"test.>"},
			AllowBatchPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// Create a fast publisher with continue on gap enabled
		batch, err := jetstreamext.NewFastPublisher(js,
			jetstreamext.WithFastPublisherContinueOnGap(true),
		)
		if err != nil {
			t.Fatalf("Unexpected error creating fast publisher: %v", err)
		}

		// Add messages
		for i := range 5 {
			if _, err := batch.Add("test.msg", []byte("data")); err != nil {
				t.Fatalf("Unexpected error adding message %d: %v", i, err)
			}
		}

		// Commit the batch
		ack, err := batch.Commit(ctx, "test.final", []byte("final"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		if ack.BatchSize != 6 {
			t.Fatalf("Expected BatchAck.BatchSize to be 6, got %d", ack.BatchSize)
		}
	})

	t.Run("close", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:              "TEST",
			Subjects:          []string{"test.>"},
			AllowBatchPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// Create a fast publisher
		batch, err := jetstreamext.NewFastPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating fast publisher: %v", err)
		}

		// Add messages
		if _, err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message: %v", err)
		}
		if _, err := batch.Add("test.2", []byte("message 2")); err != nil {
			t.Fatalf("Unexpected error adding message: %v", err)
		}

		// Discard the batch
		ack, err := batch.Close(context.Background())
		if err != nil {
			t.Fatalf("Unexpected error discarding batch: %v", err)
		}

		if ack.BatchSize != 2 {
			t.Fatalf("Expected BatchAck.BatchSize to be 2, got %d", ack.BatchSize)
		}

		// Verify batch is closed
		if !batch.IsClosed() {
			t.Fatal("Expected batch to be closed after discard")
		}

		// Verify we can't add more messages
		if _, err := batch.Add("test.3", []byte("message 3")); !errors.Is(err, jetstreamext.ErrBatchClosed) {
			t.Fatalf("Expected ErrBatchClosed adding to discarded batch, got %v", err)
		}
	})
}

func TestFastPublisher_ReplyPrefixUnchangedOnFlowChange(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)
	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"test.>"},
		AllowBatchPublish: true,
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	// nc2 injects one flow ack with a lower flow value, if the reply subject is rewritten
	// that shows the initial flow value is lost.
	nc2 := client(t, s)
	defer nc2.Close()
	injectOnce := sync.Once{}
	_, err = nc2.Subscribe("_INBOX.>", func(msg *nats.Msg) {
		var ack struct {
			Type     string `json:"type"`
			Sequence uint64 `json:"seq"`
			Messages uint16 `json:"msgs"`
		}
		if json.Unmarshal(msg.Data, &ack) != nil || ack.Type != "ack" {
			return
		}
		injectOnce.Do(func() {
			ack.Messages = 1
			data, _ := json.Marshal(ack)
			_ = nc2.Publish(msg.Subject, data)
		})
	})
	if err != nil {
		t.Fatalf("Unexpected error subscribing: %v", err)
	}

	nc3 := client(t, s)
	defer nc3.Close()
	sub, err := nc3.SubscribeSync("test.msg")
	if err != nil {
		t.Fatalf("Unexpected error subscribing: %v", err)
	}
	if err = nc3.Flush(); err != nil {
		t.Fatalf("Unexpected error flushing: %v", err)
	}

	batch, err := jetstreamext.NewFastPublisher(js,
		jetstreamext.FastPublishFlowControl{
			Flow:               10,
			MaxOutstandingAcks: 5,
			AckTimeout:         5 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("Unexpected error creating publisher: %v", err)
	}

	const msgCount = 200
	for i := range msgCount {
		if _, err := batch.Add("test.msg", []byte("data")); err != nil {
			t.Fatalf("Unexpected error adding message %d: %v", i, err)
		}

		// Inspect the reply subject of the messages that are published.
		rmsg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error receiving message: %v", err)
		}
		if !strings.Contains(rmsg.Reply, ".10.fail.") {
			t.Fatalf("Initial flow in reply subject was overwritten: %q", rmsg.Reply)
		}
	}

	ack, err := batch.Commit(ctx, "test.final", []byte("final"))
	if err != nil {
		t.Fatalf("Unexpected error committing batch: %v", err)
	}
	if ack.BatchSize != msgCount+1 {
		t.Fatalf("Expected BatchSize %d, got %d", msgCount+1, ack.BatchSize)
	}
}

func TestFastPublisher_LargeBatch(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	// Create a stream with batch publishing enabled
	cfg := jetstream.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"test.>"},
		AllowBatchPublish: true,
	}
	_, err := js.CreateStream(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	// Create a fast publisher with flow of 100
	batch, err := jetstreamext.NewFastPublisher(js,
		jetstreamext.FastPublishFlowControl{
			Flow:               100,
			MaxOutstandingAcks: 2,
			AckTimeout:         2 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("Unexpected error creating fast publisher: %v", err)
	}

	// Add multiple messages
	for i := range 100000 {
		if _, err := batch.Add("test.msg", []byte("data")); err != nil {
			t.Fatalf("Unexpected error adding message %d: %v", i, err)
		}
	}

	// Commit the batch
	ack, err := batch.Close(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error committing batch: %v", err)
	}

	if ack.BatchSize != 100000 {
		t.Fatalf("Expected BatchAck.BatchSize to be 100000, got %d", ack.BatchSize)
	}
}

func TestFastPublisher_WaitForAckOnExactBoundary(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)
	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"test.>"},
		AllowBatchPublish: true,
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	// flow=1: ack every message, at most 1 outstanding ack at a time.
	batch, err := jetstreamext.NewFastPublisher(js,
		jetstreamext.FastPublishFlowControl{
			Flow:               1,
			MaxOutstandingAcks: 1,
			AckTimeout:         5 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("Unexpected error creating publisher: %v", err)
	}

	// Message 1 goes through the first-ack path and always waits.
	_, err = batch.Add("test.msg", []byte("data"))
	if err != nil {
		t.Fatalf("Unexpected error adding msg 1: %v", err)
	}

	// Message 2: seq=2, ackSeq=1. Stall condition: 1+1 <= 2 = true.
	// With old code (1+1 < 2 = false): no stall, AckSequence returned as 1. Additionally,
	// if we would stall the ping timer would kick in and fail the batch due to there being a gap
	// since the message wasn't sent right after increasing the sequence.
	// With new code: sends first, stalls, waits for the server ack, AckSequence returned as 2.
	ack2, err := batch.Add("test.msg", []byte("data"))
	if err != nil {
		t.Fatalf("Unexpected error adding msg 2: %v", err)
	}
	if ack2.AckSequence < ack2.BatchSequence {
		t.Errorf("AckSequence %d < BatchSequence %d: publisher did not stall at the exact flow boundary; old code used '<' which skipped the stall when outstanding equalled flow*maxOA", ack2.AckSequence, ack2.BatchSequence)
	}

	if _, err = batch.Commit(ctx, "test.commit", []byte("commit")); err != nil {
		t.Fatalf("Unexpected error committing: %v", err)
	}
}

func TestFastPublisher_StallEveryMessage(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)
	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"test.>"},
		AllowBatchPublish: true,
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	// flow=1, MaxOutstandingAcks=1: every message triggers a stall/unstall cycle.
	// Stresses the persistent stall channel mechanics (send-not-close design
	// that allows the channel to be reused across cycles).
	batch, err := jetstreamext.NewFastPublisher(js,
		jetstreamext.FastPublishFlowControl{
			Flow:               1,
			MaxOutstandingAcks: 1,
			AckTimeout:         5 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("Unexpected error creating publisher: %v", err)
	}

	const msgCount = 50
	for i := range msgCount {
		if _, err := batch.Add("test.msg", []byte("data")); err != nil {
			t.Fatalf("Unexpected error adding message %d: %v", i, err)
		}
	}

	ack, err := batch.Commit(ctx, "test.commit", []byte("commit"))
	if err != nil {
		t.Fatalf("Unexpected error committing: %v", err)
	}
	if ack.BatchSize != msgCount+1 {
		t.Fatalf("Expected BatchSize %d, got %d", msgCount+1, ack.BatchSize)
	}
}
