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
		for i := 0; i < 50; i++ {
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
			if errors.Is(err, jetstreamext.ErrBatchPublishIncomplete) {
				// This is expected - too many outstanding batches
				return
			}
			t.Fatalf("Unexpected error adding message to batch: %v", err)
		}
		// If Add didn't fail, Commit should fail
		_, err = batch.Commit(ctx, "test.2", []byte("message 2"))
		if !errors.Is(err, jetstreamext.ErrBatchPublishIncomplete) {
			t.Fatalf("Expected ErrBatchPublishIncomplete when too many outstanding batches, got %v", err)
		}
	})

	t.Run("invalid headers", func(t *testing.T) {
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

		// Try to add message with MsgID header
		msg := &nats.Msg{
			Subject: "test.1",
			Data:    []byte("message 1"),
			Header:  nats.Header{},
		}
		msg.Header.Set(jetstream.MsgIDHeader, "test-msg-id")

		err = batch.AddMsg(msg)
		if err != nil {
			t.Fatalf("Unexpected error adding message: %v", err)
		}

		// reset headers, should still fail with appropriate error
		msg.Header = nats.Header{}
		_, err = batch.CommitMsg(ctx, msg)
		if !errors.Is(err, jetstreamext.ErrBatchPublishUnsupportedHeader) {
			t.Fatalf("Expected ErrBatchPublishUnsupportedHeader, got %v", err)
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
		for i := 0; i < 999; i++ {
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

		for i := 0; i < 1000; i++ {
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
		if ack.BatchSize != count {
			t.Fatalf("Expected BatchAck.BatchSize to be %d, got %d", count, ack.BatchSize)
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
