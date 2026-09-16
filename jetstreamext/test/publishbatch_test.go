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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

func TestBatchPublisher(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		stream, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// Create a batch publisher
		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
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

		// check stream info to verify no messages yet
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 0 {
			t.Fatalf("Expected 0 messages in the stream, got %d", info.State.Msgs)
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

		// Verify batch is closed
		if !batch.IsClosed() {
			t.Fatal("Expected batch to be closed after commit")
		}

		// Verify we can't add more messages
		if err := batch.Add("test.4", []byte("message 4")); err == nil {
			t.Fatal("Expected error adding to closed batch")
		}

		// verify we have 3 messages in the stream
		info, err = stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 3 {
			t.Fatalf("Expected 3 messages in the stream, got %d", info.State.Msgs)
		}
	})

	t.Run("with options", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing and TTL enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
			AllowMsgTTL:        true,
		}
		stream, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		for range 5 {
			if _, err := js.Publish(ctx, "test.foo", []byte("hello")); err != nil {
				t.Fatalf("Unexpected error publishing message: %v", err)
			}
		}
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 5 {
			t.Fatalf("Expected 5 messages in the stream, got %d", info.State.Msgs)
		}
		time.Sleep(time.Second)

		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		// Add first message with TTL and ExpectLastSequence (allowed on first message)
		if err := batch.Add("test.1", []byte("message 1"), jetstreamext.WithBatchMsgTTL(5*time.Second), jetstreamext.WithBatchExpectLastSequence(5)); err != nil {
			t.Fatalf("Unexpected error adding first message with options: %v", err)
		}

		// Add second message with expected stream (no ExpectLastSequence)
		if err := batch.AddMsg(&nats.Msg{
			Subject: "test.2",
			Data:    []byte("message 2"),
		}, jetstreamext.WithBatchExpectStream("TEST")); err != nil {
			t.Fatalf("Unexpected error adding second message with expected stream: %v", err)
		}

		// Commit third message
		ack, err := batch.Commit(ctx, "test.3", []byte("message 3"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch with expected sequence: %v", err)
		}

		if ack == nil {
			t.Fatal("Expected non-nil BatchAck")
		}

		// Verify ack contains expected stream
		if ack.Stream != "TEST" {
			t.Fatalf("Expected stream name to be TEST, got %s", ack.Stream)
		}

		info, err = stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 8 {
			t.Fatalf("Expected 8 messages in the stream, got %d", info.State.Msgs)
		}
	})

	t.Run("expect last sequence validation", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		// First message with ExpectLastSequence should work
		if err := batch.Add("test.1", []byte("message 1"), jetstreamext.WithBatchExpectLastSequence(0)); err != nil {
			t.Fatalf("Unexpected error adding first message with ExpectLastSequence: %v", err)
		}

		ack, err := batch.Commit(ctx, "test.2", []byte("message 2"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		if ack == nil {
			t.Fatal("Expected non-nil BatchAck")
		}
	})

	t.Run("invalid last sequence", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)
		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}
		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}
		// First message with invalid ExpectLastSequence should fail
		_, err = batch.Commit(ctx, "test.1", []byte("message 1"), jetstreamext.WithBatchExpectLastSequence(5))
		if err == nil {
			t.Fatal("Expected error committing with invalid ExpectLastSequence")
		}
		var apiErr *jetstream.APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("Expected APIError, got %v", err)
		}
		if apiErr.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence {
			t.Fatalf("Expected error %d, got %d", jetstream.JSErrCodeStreamWrongLastSequence, apiErr.ErrorCode)
		}
	})

	t.Run("too many outstanding batches", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// create 50 batches (the default max) and add a message to each
		for range 50 {
			batch, err := jetstreamext.NewBatchPublisher(js)
			if err != nil {
				t.Fatalf("Unexpected error creating batch publisher: %v", err)
			}
			err = batch.Add("test.1", []byte("message 1"))
			if err != nil {
				t.Fatalf("Unexpected error adding message to batch: %v", err)
			}
		}
		// Now create one more batch
		// With flow control, the error might come on Add (if WaitFirst=true) or Commit
		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}
		err = batch.Add("test.1", []byte("message 1"))
		if err != nil {
			// With flow control enabled (WaitFirst=true by default), the first Add may fail
			if errors.Is(err, jetstreamext.ErrBatchPublishIncomplete) || errors.Is(err, jetstreamext.ErrAtomicPublishTooManyInflight) || errors.Is(err, jetstreamext.ErrBatchPublishTooManyInflight) {
				// This is expected - too many outstanding batches
				return
			}
			t.Fatalf("Unexpected error adding message to batch: %v", err)
		}
		// If Add didn't fail, Commit should fail
		_, err = batch.Commit(ctx, "test.2", []byte("message 2"))
		if !errors.Is(err, jetstreamext.ErrBatchPublishIncomplete) && !errors.Is(err, jetstreamext.ErrAtomicPublishTooManyInflight) && !errors.Is(err, jetstreamext.ErrBatchPublishTooManyInflight) {
			t.Fatalf("Expected too many inflight error when too many outstanding batches, got %v", err)
		}
	})

	t.Run("batch too large", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		// Add messages until we exceed the max batch size (1000 messages)
		for range 999 {
			err = batch.Add("test.1", []byte("message 1"))
			if err != nil {
				t.Fatalf("Unexpected error adding message to batch: %v", err)
			}
		}

		// commit is msg 1000 (within limit)
		_, err = batch.Commit(ctx, "test.2", []byte("message 2"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		// Try to create another batch and add 1001 messages
		batch2, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating second batch publisher: %v", err)
		}

		for range 1000 {
			err = batch2.Add("test.1", []byte("message 1"))
			if err != nil {
				t.Fatalf("Unexpected error adding message to batch: %v", err)
			}
		}

		// This should be message 1001 and should fail with exceeds limit error
		_, err = batch2.Commit(ctx, "test.2", []byte("message 2"))
		if !errors.Is(err, jetstreamext.ErrBatchPublishExceedsLimit) {
			t.Fatalf("Expected ErrBatchPublishExceedsLimit, got %v", err)
		}
	})

	t.Run("batch publish not enabled", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create a stream WITHOUT batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// Create batch publisher with flow control enabled
		batch, err := jetstreamext.NewBatchPublisher(js, jetstreamext.BatchFlowControl{
			AckFirst:   true,
			AckTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}
		// First message should fail with batch publish not enabled
		err = batch.Add("test.1", []byte("message 1"))
		if !errors.Is(err, jetstreamext.ErrBatchPublishNotEnabled) {
			t.Fatalf("Expected ErrBatchPublishNotEnabled, got %v", err)
		}
	})

	t.Run("with ack every flow control", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		stream, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := jetstreamext.NewBatchPublisher(js, jetstreamext.BatchFlowControl{
			AckFirst:   true,
			AckEvery:   2,
			AckTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		for i := range 5 {
			if err := batch.Add("test.1", fmt.Appendf(nil, "message %d", i+1)); err != nil {
				t.Fatalf("Unexpected error adding message %d: %v", i+1, err)
			}
		}

		// Six messages total: five added under flow control, plus the
		// final one that carries the commit.
		ack, err := batch.Commit(ctx, "test.1", []byte("message 6"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}
		if ack.BatchSize != 6 {
			t.Fatalf("Expected BatchAck.BatchSize to be 6, got %d", ack.BatchSize)
		}

		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 6 {
			t.Fatalf("Expected 6 messages in the stream, got %d", info.State.Msgs)
		}
	})

	t.Run("does not modify message", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)
		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		if _, err := js.CreateStream(ctx, cfg); err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		msg := nats.NewMsg("test.1")
		msg.Header.Set("X-User", "value")
		if err := batch.AddMsg(msg, jetstreamext.WithBatchExpectLastSequence(0)); err != nil {
			t.Fatalf("Unexpected error adding message: %v", err)
		}
		commit := nats.NewMsg("test.2")
		if _, err := batch.CommitMsg(ctx, commit, jetstreamext.WithBatchExpectStream("TEST")); err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		if msg.Reply != "" || len(msg.Header) != 1 || msg.Header.Get("X-User") != "value" {
			t.Fatalf("Expected added message to be unmodified, got reply %q, headers %v", msg.Reply, msg.Header)
		}
		if commit.Reply != "" || len(commit.Header) != 0 {
			t.Fatalf("Expected commit message to be unmodified, got reply %q, headers %v", commit.Reply, commit.Header)
		}
	})

}

func TestBatchPublisher_Discard(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)
	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a stream with batch publishing enabled
	cfg := jetstream.StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"test.>"},
		AllowAtomicPublish: true,
	}
	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	batch, err := jetstreamext.NewBatchPublisher(js)
	if err != nil {
		t.Fatalf("Unexpected error creating batch publisher: %v", err)
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

	// Discard the batch
	if err := batch.Discard(); err != nil {
		t.Fatalf("Unexpected error discarding batch: %v", err)
	}

	// try discarding again
	err = batch.Discard()
	if !errors.Is(err, jetstreamext.ErrBatchClosed) {
		t.Fatalf("Expected ErrBatchClosed discarding already closed batch, got %v", err)
	}

	// Verify batch is closed
	if !batch.IsClosed() {
		t.Fatal("Expected batch to be closed after discard")
	}

	// Verify we can't add more messages
	if err := batch.Add("test.4", []byte("message 4")); err == nil {
		t.Fatal("Expected error adding to closed batch")
	}

	// Verify we can't commit
	_, err = batch.Commit(ctx, "test.5", []byte("message 5"))
	if !errors.Is(err, jetstreamext.ErrBatchClosed) {
		t.Fatalf("Expected error committing closed batch, got %v", err)
	}

	// verify we have 0 messages in the stream
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error getting stream info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("Expected 0 messages in the stream, got %d", info.State.Msgs)
	}
}

func TestBatchPublisher_Close(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		stream, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		if err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message 1: %v", err)
		}
		if err := batch.AddMsg(&nats.Msg{
			Subject: "test.2",
			Data:    []byte("message 2"),
		}); err != nil {
			t.Fatalf("Unexpected error adding message 2: %v", err)
		}
		if err := batch.Add("test.3", []byte("message 3")); err != nil {
			t.Fatalf("Unexpected error adding message 3: %v", err)
		}

		// Commit via an end-of-batch marker; the marker is not stored.
		ack, err := batch.Close(ctx)
		if err != nil {
			t.Fatalf("Unexpected error closing batch: %v", err)
		}

		if ack.Stream != "TEST" {
			t.Fatalf("Expected ack stream TEST, got %q", ack.Stream)
		}
		// BatchSize counts messages only, excluding the EOB marker.
		if ack.BatchSize != 3 {
			t.Fatalf("Expected BatchAck.BatchSize to be 3, got %d", ack.BatchSize)
		}
		// Sequence is the stream sequence of the last stored message.
		if ack.Sequence != 3 {
			t.Fatalf("Expected BatchAck.Sequence to be 3, got %d", ack.Sequence)
		}
		if ack.BatchID == "" {
			t.Fatal("Expected BatchAck.BatchID to be set")
		}

		// The marker must not have been stored.
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 3 {
			t.Fatalf("Expected 3 messages in the stream, got %d", info.State.Msgs)
		}

		// The server rewrites the last stored message to carry the regular
		// commit header, even though the batch was committed via EOB, and
		// leaves the preceding messages alone.
		last, err := stream.GetMsg(ctx, 3)
		if err != nil {
			t.Fatalf("Unexpected error getting last message: %v", err)
		}
		if got := last.Header.Get(jetstreamext.BatchCommitHeader); got != "1" {
			t.Fatalf("Expected last message to have %s=1, got %q", jetstreamext.BatchCommitHeader, got)
		}
		if got := last.Header.Get(jetstreamext.BatchSeqHeader); got != "3" {
			t.Fatalf("Expected last message to have %s=3, got %q", jetstreamext.BatchSeqHeader, got)
		}
		prev, err := stream.GetMsg(ctx, 2)
		if err != nil {
			t.Fatalf("Unexpected error getting message 2: %v", err)
		}
		if got := prev.Header.Get(jetstreamext.BatchCommitHeader); got != "" {
			t.Fatalf("Expected message 2 to have no %s header, got %q", jetstreamext.BatchCommitHeader, got)
		}

		// Size reports messages added, not the marker's sequence.
		if batch.Size() != 3 {
			t.Fatalf("Expected Size() to be 3, got %d", batch.Size())
		}
		if !batch.IsClosed() {
			t.Fatal("Expected batch to be closed after Close")
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		if _, err := js.CreateStream(ctx, cfg); err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		// No messages added; the server would abandon this batch, so it is
		// rejected client-side without a round trip.
		if _, err := batch.Close(ctx); !errors.Is(err, jetstreamext.ErrEmptyBatch) {
			t.Fatalf("Expected ErrEmptyBatch closing an empty batch, got %v", err)
		}

		// The batch is still usable after a rejected Close.
		if batch.IsClosed() {
			t.Fatal("Expected batch not to be closed after a rejected Close")
		}
	})

	t.Run("close after commit", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		if _, err := js.CreateStream(ctx, cfg); err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		if err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message 1: %v", err)
		}
		if _, err := batch.Commit(ctx, "test.2", []byte("message 2")); err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		if _, err := batch.Close(ctx); !errors.Is(err, jetstreamext.ErrBatchClosed) {
			t.Fatalf("Expected ErrBatchClosed closing a committed batch, got %v", err)
		}
	})

	t.Run("close after discard", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		if _, err := js.CreateStream(ctx, cfg); err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		if err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message 1: %v", err)
		}
		if err := batch.Discard(); err != nil {
			t.Fatalf("Unexpected error discarding batch: %v", err)
		}

		if _, err := batch.Close(ctx); !errors.Is(err, jetstreamext.ErrBatchClosed) {
			t.Fatalf("Expected ErrBatchClosed closing a discarded batch, got %v", err)
		}
	})

	t.Run("close twice", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		stream, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := jetstreamext.NewBatchPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		if err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message 1: %v", err)
		}
		if _, err := batch.Close(ctx); err != nil {
			t.Fatalf("Unexpected error closing batch: %v", err)
		}

		if _, err := batch.Close(ctx); !errors.Is(err, jetstreamext.ErrBatchClosed) {
			t.Fatalf("Expected ErrBatchClosed closing an already closed batch, got %v", err)
		}

		// Adding after Close is rejected too.
		if err := batch.Add("test.2", []byte("message 2")); !errors.Is(err, jetstreamext.ErrBatchClosed) {
			t.Fatalf("Expected ErrBatchClosed adding to a closed batch, got %v", err)
		}

		// The second Close must not have stored anything extra.
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 1 {
			t.Fatalf("Expected 1 message in the stream, got %d", info.State.Msgs)
		}
	})

	t.Run("server rejects the commit", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// AllowAtomicPublish deliberately left off, so the server rejects
		// the batch when it is committed.
		cfg := jetstream.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
		}
		if _, err := js.CreateStream(ctx, cfg); err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// AckFirst disabled so Add publishes fire-and-forget and the
		// rejection surfaces on Close rather than on the first Add.
		batch, err := jetstreamext.NewBatchPublisher(js, jetstreamext.BatchFlowControl{
			AckFirst:   false,
			AckTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		if err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message 1: %v", err)
		}

		if _, err := batch.Close(ctx); !errors.Is(err, jetstreamext.ErrBatchPublishNotEnabled) {
			t.Fatalf("Expected ErrBatchPublishNotEnabled closing batch, got %v", err)
		}

		// A server-rejected commit still closes the batch: the server has
		// abandoned it, so it cannot be retried.
		if !batch.IsClosed() {
			t.Fatal("Expected batch to be closed after a rejected Close")
		}
	})
}

func TestPublishMsgBatch(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)
		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		stream, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		count := 100
		messages := make([]*nats.Msg, 0, count)
		for range count {
			messages = append(messages, &nats.Msg{
				Subject: "test.subject",
				Data:    []byte("message"),
				Header:  nats.Header{},
			})
		}

		ack, err := jetstreamext.PublishMsgBatch(ctx, js, messages)
		if err != nil {
			t.Fatalf("Unexpected error publishing message batch: %v", err)
		}
		if ack == nil {
			t.Fatal("Expected non-nil BatchAck")
		}

		// verify we have 100 messages in the stream
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != uint64(count) {
			t.Fatalf("Expected %d messages in the stream, got %d", count, info.State.Msgs)
		}
		if ack.BatchSize != uint64(count) {
			t.Fatalf("Expected BatchAck.BatchSize to be %d, got %d", count, ack.BatchSize)
		}
	})
	t.Run("does not modify messages", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)
		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		if _, err := js.CreateStream(ctx, cfg); err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// The first message has no header map at all, which used to panic.
		messages := []*nats.Msg{
			{Subject: "test.1", Data: []byte("message 1")},
			nats.NewMsg("test.2"),
		}
		messages[1].Header.Set("X-User", "value")
		if _, err := jetstreamext.PublishMsgBatch(ctx, js, messages); err != nil {
			t.Fatalf("Unexpected error publishing batch: %v", err)
		}

		if messages[0].Header != nil || messages[0].Reply != "" {
			t.Fatalf("Expected first message to be unmodified, got reply %q, headers %v", messages[0].Reply, messages[0].Header)
		}
		if len(messages[1].Header) != 1 || messages[1].Header.Get("X-User") != "value" {
			t.Fatalf("Expected second message to be unmodified, got headers %v", messages[1].Header)
		}
	})

	t.Run("too many messages", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)
		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		count := 1001
		messages := make([]*nats.Msg, 0, count)
		for range count {
			messages = append(messages, &nats.Msg{
				Subject: "test.subject",
				Data:    []byte("message"),
				Header:  nats.Header{},
			})
		}

		_, err = jetstreamext.PublishMsgBatch(ctx, js, messages)
		if !errors.Is(err, jetstreamext.ErrBatchPublishExceedsLimit) {
			t.Fatalf("Expected ErrBatchPublishExceedsLimit publishing too many messages, got %v", err)
		}
	})
}
