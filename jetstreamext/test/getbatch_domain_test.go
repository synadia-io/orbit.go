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
	"iter"
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

// TestGetBatchWithDomain checks that batch direct gets reach a server whose
// JetStream runs under a domain, through every way of addressing it: a
// context created with the domain, one created with the equivalent API
// prefix, and the default context (unprefixed subjects always mean the
// connected server's own JetStream).
func TestGetBatchWithDomain(t *testing.T) {
	const domain = "hub"
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.JetStreamDomain = domain
	srv := RunServerWithOptions(&opts)
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.NewWithDomain(nc, domain)
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo.*"},
		AllowDirect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// pub order (seq): foo.A (1), foo.B (2), foo.A (3), foo.B (4), foo.C (5)
	for _, subject := range []string{"foo.A", "foo.B", "foo.A", "foo.B", "foo.C"} {
		if _, err := js.Publish(context.Background(), subject, []byte("msg")); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	withPrefix, err := jetstream.NewWithAPIPrefix(nc, "$JS."+domain+".API")
	if err != nil {
		t.Fatal(err)
	}
	withDefault, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	contexts := []struct {
		name string
		js   jetstream.JetStream
	}{
		{name: "with domain", js: js},
		{name: "with API prefix", js: withPrefix},
		{name: "default", js: withDefault},
	}

	for _, c := range contexts {
		t.Run(c.name, func(t *testing.T) {
			t.Run("GetBatch", func(t *testing.T) {
				batch, err := jetstreamext.GetBatch(context.Background(), c.js, "TEST", 10)
				if err != nil {
					t.Fatalf("Failed to get batch: %v", err)
				}
				expectedSeqs := []uint64{1, 2, 3, 4, 5}
				checkSequences(t, batch, expectedSeqs)
			})

			t.Run("GetLastMsgsFor", func(t *testing.T) {
				batch, err := jetstreamext.GetLastMsgsFor(context.Background(), c.js, "TEST", []string{"foo.*"})
				if err != nil {
					t.Fatalf("Failed to get last messages: %v", err)
				}
				expectedSeqs := []uint64{3, 4, 5}
				checkSequences(t, batch, expectedSeqs)
			})
		})
	}

	// A domain the server does not have is not answered.
	t.Run("unknown domain", func(t *testing.T) {
		other, err := jetstream.NewWithDomain(nc, "other")
		if err != nil {
			t.Fatal(err)
		}
		batch, err := jetstreamext.GetBatch(context.Background(), other, "TEST", 10)
		if err != nil {
			t.Fatalf("Failed to get batch: %v", err)
		}
		for _, err := range batch {
			if !errors.Is(err, nats.ErrNoResponders) {
				t.Fatalf("Expected error %v, got %v", nats.ErrNoResponders, err)
			}
			return
		}
		t.Fatal("Expected an error from the batch")
	})
}

// checkSequences drains the batch and compares the sequences of the
// messages it yields with the expected ones.
func checkSequences(t *testing.T, batch iter.Seq2[*jetstream.RawStreamMsg, error], expectedSeqs []uint64) {
	t.Helper()
	var i int
	for msg, err := range batch {
		if err != nil {
			t.Fatal(err)
		}
		if i >= len(expectedSeqs) {
			t.Fatalf("Received more messages than expected")
		}
		if msg.Sequence != expectedSeqs[i] {
			t.Fatalf("Expected sequence %d, got %d", expectedSeqs[i], msg.Sequence)
		}
		i++
	}
	if i != len(expectedSeqs) {
		t.Fatalf("Expected %d messages, got %d", len(expectedSeqs), i)
	}
}
