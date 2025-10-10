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

package test

import (
	"context"
	"errors"
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
		if err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message 1: %v", err)
		}

		if err := batch.AddMsg(&nats.Msg{
			Subject: "test.2",
			Data:    []byte("message 2"),
		}); err != nil {
			t.Fatalf("Unexpected error adding message 2: %v", err)
		}

		// Check size
		if size := batch.Size(); size != 2 {
			t.Fatalf("Expected batch size to be 2, got %d", size)
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
		if err := batch.Add("test.4", []byte("message 4")); !errors.Is(err, jetstreamext.ErrBatchClosed) {
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
				MaxOutstandingAcks: 10,
				AckTimeout:         3 * time.Second,
			},
		)
		if err != nil {
			t.Fatalf("Unexpected error creating fast publisher: %v", err)
		}

		// Add multiple messages
		for i := range 200 {
			if err := batch.Add("test.msg", []byte("data")); err != nil {
				t.Fatalf("Unexpected error adding message %d: %v", i, err)
			}
		}

		// Commit the batch
		ack, err := batch.Commit(ctx, "test.final", []byte("final"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		if ack.BatchSize != 201 {
			t.Fatalf("Expected BatchAck.BatchSize to be 21, got %d", ack.BatchSize)
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
			if err := batch.Add("test.msg", []byte("data")); err != nil {
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

	t.Run("discard", func(t *testing.T) {
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
		if err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message: %v", err)
		}
		if err := batch.Add("test.2", []byte("message 2")); err != nil {
			t.Fatalf("Unexpected error adding message: %v", err)
		}

		// Discard the batch
		if err := batch.Discard(); err != nil {
			t.Fatalf("Unexpected error discarding batch: %v", err)
		}

		// Verify batch is closed
		if !batch.IsClosed() {
			t.Fatal("Expected batch to be closed after discard")
		}

		// Verify we can't add more messages
		if err := batch.Add("test.3", []byte("message 3")); !errors.Is(err, jetstreamext.ErrBatchClosed) {
			t.Fatalf("Expected ErrBatchClosed adding to discarded batch, got %v", err)
		}
	})
}

func TestFastPublisher_LargeBatch(t *testing.T) {
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

	// Create a fast publisher with flow of 1
	batch, err := jetstreamext.NewFastPublisher(js,
		jetstreamext.FastPublishFlowControl{
			Flow:               100,
			MaxOutstandingAcks: 50,
			AckTimeout:         2 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("Unexpected error creating fast publisher: %v", err)
	}

	// Add multiple messages
	for i := range 100000 {
		if err := batch.Add("test.msg", []byte("data")); err != nil {
			t.Fatalf("Unexpected error adding message %d: %v", i, err)
		}
	}
	// Commit the batch
	ack, err := batch.Commit(ctx, "test.final", []byte("final"))
	if err != nil {
		t.Fatalf("Unexpected error committing batch: %v", err)
	}

	if ack.BatchSize != 100001 {
		t.Fatalf("Expected BatchAck.BatchSize to be 1001, got %d", ack.BatchSize)
	}
}
