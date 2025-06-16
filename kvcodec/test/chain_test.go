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
	"maps"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/kvcodec"
)

func TestChainCodecs(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	pathCodec := kvcodec.PathCodec()
	base64Codec := kvcodec.Base64Codec()
	noOpCodec := kvcodec.NoOpCodec()

	keyChain, err := kvcodec.NewKeyChainCodec(pathCodec, base64Codec)
	if err != nil {
		t.Fatalf("failed to create key chain codec: %v", err)
	}

	valueChain, err := kvcodec.NewValueChainCodec(base64Codec, noOpCodec)
	if err != nil {
		t.Fatalf("failed to create value chain codec: %v", err)
	}

	basicKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      "basic-test",
		Description: "Basic operations test",
	})
	if err != nil {
		t.Fatalf("failed to create basic bucket: %v", err)
	}

	watchKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      "watch-test",
		Description: "Watch test",
	})
	if err != nil {
		t.Fatalf("failed to create watch bucket: %v", err)
	}
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	basicCodecKV := kvcodec.New(basicKV, keyChain, valueChain)
	watchCodecKV := kvcodec.New(watchKV, keyChain, valueChain)

	t.Run("basic operations", func(t *testing.T) {
		key := "/config/app/database/host"
		value := []byte("localhost")

		_, err = basicCodecKV.Put(ctx, key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		entry, err := basicCodecKV.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if entry.Key() != key {
			t.Errorf("key mismatch: expected %s, got %s", key, entry.Key())
		}

		if string(entry.Value()) != string(value) {
			t.Errorf("value mismatch: expected %s, got %s", value, entry.Value())
		}
	})

	t.Run("watcher", func(t *testing.T) {
		pattern := "/config/*/database"

		putKeys := map[string]struct{}{
			"/config/app/database":    {},
			"/config/api/database":    {},
			"/config/worker/database": {},
		}
		expectedKeys := map[string]struct{}{
			"config/app/database":    {},
			"config/api/database":    {},
			"config/worker/database": {},
		}

		watcher, err := watchCodecKV.Watch(ctx, pattern, jetstream.UpdatesOnly())
		if err != nil {
			t.Fatalf("Watch failed: %v", err)
		}
		defer watcher.Stop()

		for key := range putKeys {
			_, err = watchCodecKV.Put(ctx, key, []byte("test-value"))
			if err != nil {
				t.Fatalf("Put failed for key %s: %v", key, err)
			}
		}

		received := make(map[string]struct{})
		for range len(putKeys) {
			select {
			case entry := <-watcher.Updates():
				if entry == nil {
					t.Fatal("received nil entry")
				}
				received[entry.Key()] = struct{}{}
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("Timed out waiting for updates")
			}
		}

		if !maps.Equal(received, expectedKeys) {
			t.Fatalf("Received keys do not match expected: got %v, want %v", received, expectedKeys)
		}
	})

}
