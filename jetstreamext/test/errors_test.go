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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

// TestFastBatchServerErrorCodes verifies that the fast batch error codes
// defined in jetstreamext match the codes returned by nats-server.
func TestFastBatchServerErrorCodes(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "DISABLED",
		Subjects: []string{"disabled.>"},
	}); err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              "ENABLED",
		Subjects:          []string{"enabled.>"},
		AllowBatchPublish: true,
	}); err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	// rawFastBatchPublish publishes a single message with a hand-crafted fast
	// batch reply subject and returns the error reported by the server.
	rawFastBatchPublish := func(t *testing.T, subject, batchID, gapMode string, seq, op int) error {
		t.Helper()
		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(inbox + ".>")
		if err != nil {
			t.Fatalf("Unexpected error subscribing: %v", err)
		}
		defer sub.Unsubscribe()

		reply := strings.Join([]string{inbox, batchID, "100", gapMode, strconv.Itoa(seq), strconv.Itoa(op), "$FI"}, ".")
		if err := nc.PublishMsg(&nats.Msg{Subject: subject, Reply: reply, Data: []byte("data")}); err != nil {
			t.Fatalf("Unexpected error publishing: %v", err)
		}
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("Unexpected error waiting for response: %v", err)
		}
		var resp struct {
			Error *jetstream.APIError `json:"error"`
		}
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			t.Fatalf("Unexpected error decoding response %q: %v", msg.Data, err)
		}
		if resp.Error == nil {
			t.Fatalf("Expected error response, got %q", msg.Data)
		}
		return resp.Error
	}

	t.Run("not enabled", func(t *testing.T) {
		batch, err := jetstreamext.NewFastPublisher(js)
		if err != nil {
			t.Fatalf("Unexpected error creating fast publisher: %v", err)
		}
		_, err = batch.Commit(ctx, "disabled.1", []byte("data"))
		if !errors.Is(err, jetstreamext.ErrFastBatchNotEnabled) {
			t.Fatalf("Expected ErrFastBatchNotEnabled, got %v", err)
		}
	})

	t.Run("invalid pattern", func(t *testing.T) {
		err := rawFastBatchPublish(t, "enabled.1", "batch", "bogus", 1, 0)
		if !errors.Is(err, jetstreamext.ErrFastBatchInvalidPattern) {
			t.Fatalf("Expected ErrFastBatchInvalidPattern, got %v", err)
		}
	})

	t.Run("invalid ID", func(t *testing.T) {
		err := rawFastBatchPublish(t, "enabled.1", strings.Repeat("A", 65), "fail", 1, 0)
		if !errors.Is(err, jetstreamext.ErrFastBatchInvalidID) {
			t.Fatalf("Expected ErrFastBatchInvalidID, got %v", err)
		}
	})

	t.Run("unknown ID", func(t *testing.T) {
		err := rawFastBatchPublish(t, "enabled.1", "unknown", "fail", 2, 1)
		if !errors.Is(err, jetstreamext.ErrFastBatchUnknownID) {
			t.Fatalf("Expected ErrFastBatchUnknownID, got %v", err)
		}
	})

	t.Run("mirror with batch publish", func(t *testing.T) {
		_, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:              "MIRROR",
			Mirror:            &jetstream.StreamSource{Name: "ENABLED"},
			AllowBatchPublish: true,
		})
		if !errors.Is(err, jetstreamext.ErrMirrorWithBatchPublish) {
			t.Fatalf("Expected ErrMirrorWithBatchPublish, got %v", err)
		}
	})
}
