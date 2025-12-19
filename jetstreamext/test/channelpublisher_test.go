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
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

func TestChannelPublisher_BasicFlow(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	pub, err := jetstreamext.NewChannelPublisher(js)
	if err != nil {
		t.Fatalf("Unexpected error creating channel publisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish("test.foo", []byte("message 1")); err != nil {
		t.Fatalf("Unexpected error publishing: %v", err)
	}

	select {
	case result := <-pub.Results():
		if result.Err != nil {
			t.Fatalf("Unexpected error in result: %v", result.Err)
		}
		if result.Ack == nil {
			t.Fatal("Expected non-nil ack")
		}
		if result.Ack.Stream != "TEST" {
			t.Fatalf("Expected stream TEST, got %s", result.Ack.Stream)
		}
		if result.Ack.Sequence != 1 {
			t.Fatalf("Expected sequence 1, got %d", result.Ack.Sequence)
		}
		if result.Msg == nil {
			t.Fatal("Expected non-nil message")
		}
		if result.Msg.Subject != "test.foo" {
			t.Fatalf("Expected subject test.foo, got %s", result.Msg.Subject)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for result")
	}

	// Verify pending count is 0
	if pending := pub.Pending(); pending != 0 {
		t.Fatalf("Expected 0 pending, got %d", pending)
	}
}

func TestChannelPublisher_MultipleMessages(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	pub, err := jetstreamext.NewChannelPublisher(js)
	if err != nil {
		t.Fatalf("Unexpected error creating channel publisher: %v", err)
	}
	defer pub.Close()

	count := 10
	for i := range count {
		if err := pub.Publish("test.multi", []byte("message")); err != nil {
			t.Fatalf("Unexpected error publishing message %d: %v", i, err)
		}
	}

	// Collect all results
	received := 0
	timeout := time.After(3 * time.Second)
	for range count {
		select {
		case result := <-pub.Results():
			if result.Err != nil {
				t.Fatalf("Unexpected error in result: %v", result.Err)
			}
			if result.Ack == nil {
				t.Fatal("Expected non-nil ack")
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for results, received %d/%d", received, count)
		}
	}
	// verify there are no more results
	select {
	case result := <-pub.Results():
		t.Fatalf("Expected no more results, got %v", result)
	case <-time.After(100 * time.Millisecond):
	}

}

func TestChannelPublisher_PublishError(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	pub, err := jetstreamext.NewChannelPublisher(js)
	if err != nil {
		t.Fatalf("Unexpected error creating channel publisher: %v", err)
	}
	defer pub.Close()

	// Publish with wrong expected stream using PublishOpt
	if err := pub.Publish("test.foo", []byte("message"), jetstreamext.WithExpectStream("WRONG")); err != nil {
		t.Fatalf("Unexpected error publishing: %v", err)
	}

	// Should get error in results
	select {
	case result := <-pub.Results():
		if result.Err == nil {
			t.Fatal("Expected error in result")
		}
		var apiErr *jetstream.APIError
		if !errors.As(result.Err, &apiErr) {
			t.Fatalf("Expected APIError, got %T", result.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for error result")
	}
}

func TestChannelPublisher_SemaphorePattern(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	pub, err := jetstreamext.NewChannelPublisher(js, jetstreamext.WithChannelBuffer(100))
	if err != nil {
		t.Fatalf("Unexpected error creating channel publisher: %v", err)
	}
	defer pub.Close()

	totalMessages := 100
	maxInFlight := 10
	sem := make(chan struct{}, maxInFlight)

	var wg sync.WaitGroup

	// Track errors
	publishErrors := 0
	resultErrors := 0
	successCount := 0

	// consume acks
	wg.Go(func() {
		for range totalMessages {
			result := <-pub.Results()
			if result.Err != nil {
				resultErrors++
			} else {
				successCount++
			}
			<-sem
		}
	})

	for range totalMessages {
		sem <- struct{}{}

		if err := pub.Publish("test.sem", []byte("message")); err != nil {
			publishErrors++
			<-sem
		}
	}

	wg.Wait()

	if publishErrors > 0 {
		t.Fatalf("Expected 0 publish errors, got %d", publishErrors)
	}
	if resultErrors > 0 {
		t.Fatalf("Expected 0 result errors, got %d", resultErrors)
	}
	if successCount != totalMessages {
		t.Fatalf("Expected %d successful publishes, got %d", totalMessages, successCount)
	}
}

func TestChannelPublisher_Close(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	pub, err := jetstreamext.NewChannelPublisher(js)
	if err != nil {
		t.Fatalf("Unexpected error creating channel publisher: %v", err)
	}

	// Publish some messages
	for i := range 5 {
		if err := pub.Publish("test.close", []byte("message")); err != nil {
			t.Fatalf("Unexpected error publishing message %d: %v", i, err)
		}
	}

	// Wait a bit before closing to allow acks to be sent from server
	time.Sleep(100 * time.Millisecond)

	// Close publisher - should wait for pending and close channel
	pub.Close()

	count := 0
	for range 5 {
		select {
		case result := <-pub.Results():
			if result.Err != nil {
				t.Fatalf("Unexpected error in result: %v", result.Err)
			}
			count++
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for results, received %d/5", count)
		}
	}
	if count != 5 {
		t.Errorf("Expected 5 results, got %d", count)
	}

	// Verify pending is 0
	if pending := pub.Pending(); pending != 0 {
		t.Fatalf("Expected 0 pending after close, got %d", pending)
	}

	// Close again should be safe
	pub.Close()

	// Try to publish after close
	err = pub.Publish("test.closed", []byte("message"))
	if !errors.Is(err, jetstreamext.ErrPublisherClosed) {
		t.Fatalf("Expected ErrPublisherClosed, got %v", err)
	}
}

func TestChannelPublisher_ConcurrentPublish(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	pub, err := jetstreamext.NewChannelPublisher(js)
	if err != nil {
		t.Fatalf("Unexpected error creating channel publisher: %v", err)
	}
	defer pub.Close()

	publishers := 10
	messagesPerPublisher := 20
	totalMessages := publishers * messagesPerPublisher

	// Start multiple publishers
	for range publishers {
		go func() {
			for range messagesPerPublisher {
				if err := pub.Publish("test.concurrent", []byte("message")); err != nil {
					t.Logf("Publish error: %v", err)
				}
			}
		}()
	}

	received := 0
	timeout := time.After(5 * time.Second)
	for range totalMessages {
		select {
		case result := <-pub.Results():
			if result.Err != nil {
				t.Fatalf("Unexpected error in result: %v", result.Err)
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for results, received %d/%d", received, totalMessages)
		}
	}
}

func TestChannelPublisher_WithPublishOptions(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	pub, err := jetstreamext.NewChannelPublisher(js)
	if err != nil {
		t.Fatalf("Unexpected error creating channel publisher: %v", err)
	}
	defer pub.Close()

	// Publish with expected stream option
	if err := pub.Publish("test.opts", []byte("message"), jetstreamext.WithExpectStream("TEST")); err != nil {
		t.Fatalf("Unexpected error publishing: %v", err)
	}

	// Verify result
	select {
	case result := <-pub.Results():
		if result.Err != nil {
			t.Fatalf("Unexpected error in result: %v", result.Err)
		}
		if result.Ack == nil {
			t.Fatal("Expected non-nil ack")
		}
		if result.Ack.Stream != "TEST" {
			t.Fatalf("Expected stream TEST, got %s", result.Ack.Stream)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for result")
	}

	// Publish with message ID option
	msgID := "unique-id-123"
	if err := pub.Publish("test.opts", []byte("message 2"), jetstreamext.WithMsgID(msgID)); err != nil {
		t.Fatalf("Unexpected error publishing with msg ID: %v", err)
	}

	// Verify result
	select {
	case result := <-pub.Results():
		if result.Err != nil {
			t.Fatalf("Unexpected error in result: %v", result.Err)
		}
		if result.Ack == nil {
			t.Fatal("Expected non-nil ack")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for result with msg ID")
	}

	// Try to publish duplicate message ID - should be deduplicated
	if err := pub.Publish("test.opts", []byte("duplicate"), jetstreamext.WithMsgID(msgID)); err != nil {
		t.Fatalf("Unexpected error publishing duplicate: %v", err)
	}

	// should get ack with Duplicate=true
	select {
	case result := <-pub.Results():
		if result.Err != nil {
			t.Fatalf("Unexpected error for duplicate: %v", result.Err)
		}
		if result.Ack == nil {
			t.Fatal("Expected non-nil ack for duplicate")
		}
		if !result.Ack.Duplicate {
			t.Fatal("Expected duplicate flag to be true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for duplicate result")
	}
}

func TestChannelPublisher_WithChannelBuffer(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a stream
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	// Create with small buffer
	pub, err := jetstreamext.NewChannelPublisher(js, jetstreamext.WithChannelBuffer(5))
	if err != nil {
		t.Fatalf("Unexpected error creating channel publisher: %v", err)
	}
	defer pub.Close()

	// Publish messages
	for i := range 5 {
		if err := pub.Publish("test.buffer", []byte("message")); err != nil {
			t.Fatalf("Unexpected error publishing message %d: %v", i, err)
		}
	}

	// Read all results
	for range 5 {
		select {
		case result := <-pub.Results():
			if result.Err != nil {
				t.Fatalf("Unexpected error in result: %v", result.Err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for result")
		}
	}
}

func TestChannelPublisher_InvalidOptions(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	// Invalid buffer size
	_, err := jetstreamext.NewChannelPublisher(js, jetstreamext.WithChannelBuffer(0))
	if !errors.Is(err, jetstreamext.ErrInvalidOption) {
		t.Fatalf("Expected ErrInvalidOption, got %v", err)
	}

	_, err = jetstreamext.NewChannelPublisher(js, jetstreamext.WithChannelBuffer(-1))
	if !errors.Is(err, jetstreamext.ErrInvalidOption) {
		t.Fatalf("Expected ErrInvalidOption for negative buffer, got %v", err)
	}
}

func TestChannelPublisher_MaxPending(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(5))
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
		NoAck:    true, // Use NoAck to force server to not send acks back
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	pub, err := jetstreamext.NewChannelPublisher(js)
	if err != nil {
		t.Fatalf("Unexpected error creating channel publisher: %v", err)
	}
	defer pub.Close()

	// Publish up to max pending
	for i := range 5 {
		if err := pub.Publish("test.maxpending", []byte("message")); err != nil {
			t.Fatalf("Unexpected error publishing message %d: %v", i, err)
		}
	}
	if pub.Pending() != 5 {
		t.Fatalf("Expected 5 pending, got %d", pub.Pending())
	}

	// Try to publish one more - should hit max and stall
	start := time.Now()
	err = pub.Publish("test.maxpending", []byte("message"), jetstreamext.WithStallWait(100*time.Millisecond))
	elapsed := time.Since(start)

	if !errors.Is(err, jetstreamext.ErrTooManyStalledMsgs) {
		t.Fatalf("Expected ErrTooManyStalledMsgs, got %v", err)
	}
	if elapsed < 80*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Fatalf("Stall wait took %v, expected ~100ms", elapsed)
	}
}

func TestChannelPublisher_AckTimeout(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a stream
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.>"},
		NoAck:    true, // Use NoAck to force server to not send acks back
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	// Create JetStream with short publish async timeout
	jsWithTimeout, err := jetstream.New(nc, jetstream.WithPublishAsyncTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error creating jetstream: %v", err)
	}

	pub, err := jetstreamext.NewChannelPublisher(jsWithTimeout)
	if err != nil {
		t.Fatalf("Unexpected error creating channel publisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish("test.timeout", []byte("message")); err != nil {
		t.Fatalf("Unexpected error publishing: %v", err)
	}
	if pub.Pending() != 1 {
		t.Fatalf("Expected 1 pending, got %d", pub.Pending())
	}

	select {
	case result := <-pub.Results():
		if result.Err == nil {
			t.Fatal("Expected timeout error in result")
		}
		if !errors.Is(result.Err, jetstreamext.ErrAckTimeout) {
			t.Fatalf("Expected ErrAckTimeout, got %v", result.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for result")
	}

	// Verify Pending() returns 0 after ack times out
	if pending := pub.Pending(); pending != 0 {
		t.Fatalf("Expected 0 pending, got %d", pending)
	}
}
