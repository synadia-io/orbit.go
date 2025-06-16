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
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/kvcodec"
)

func TestKVWithCodecsBaseOperations(t *testing.T) {
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

	tests := []struct {
		name       string
		keyCodec   kvcodec.KeyCodec
		valueCodec kvcodec.ValueCodec
	}{
		{"noop", kvcodec.NoOpCodec(), kvcodec.NoOpCodec()},
		{"base64", kvcodec.Base64Codec(), kvcodec.Base64Codec()},
		{"path", kvcodec.PathCodec(), kvcodec.NoOpCodec()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucketName := fmt.Sprintf("test_%s", tt.name)

			// Create underlying KV
			baseKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
				Bucket: bucketName,
			})
			if err != nil {
				t.Fatalf("create kv error: %v", err)
			}

			// Wrap with codecs
			kv := kvcodec.New(baseKV, tt.keyCodec, tt.valueCodec)

			t.Run("basic operations", func(t *testing.T) {
				// Put
				rev, err := kv.Put(ctx, "test.key", []byte("test value"))
				if err != nil {
					t.Fatalf("put error: %v", err)
				}
				if rev != 1 {
					t.Errorf("expected revision 1, got %d", rev)
				}

				// Get
				entry, err := kv.Get(ctx, "test.key")
				if err != nil {
					t.Fatalf("get error: %v", err)
				}
				if entry.Key() != "test.key" {
					t.Errorf("expected key 'test.key', got %s", entry.Key())
				}
				if !bytes.Equal(entry.Value(), []byte("test value")) {
					t.Errorf("expected value 'test value', got %s", entry.Value())
				}

				// Update
				rev2, err := kv.Update(ctx, "test.key", []byte("updated value"), rev)
				if err != nil {
					t.Fatalf("update error: %v", err)
				}
				if rev2 != 2 {
					t.Errorf("expected revision 2, got %d", rev2)
				}

				// Delete
				if err := kv.Delete(ctx, "test.key"); err != nil {
					t.Fatalf("delete error: %v", err)
				}

				// Get deleted
				_, err = kv.Get(ctx, "test.key")
				if err != jetstream.ErrKeyNotFound {
					t.Errorf("expected ErrKeyNotFound, got %v", err)
				}
			})

			t.Run("list", func(t *testing.T) {
				kv.Put(ctx, "list.a", []byte("a"))
				kv.Put(ctx, "list.b", []byte("b"))
				kv.Put(ctx, "list.c", []byte("c"))

				keys, err := kv.Keys(ctx)
				if err != nil {
					t.Fatalf("keys error: %v", err)
				}

				if len(keys) != 3 {
					t.Errorf("expected 3 keys, got %d", len(keys))
				}

				expectedKeys := []string{"list.a", "list.b", "list.c"}
				if tt.name == "path" {
					expectedKeys = []string{"list/a", "list/b", "list/c"}
				}
				if !slices.Equal(keys, expectedKeys) {
					t.Errorf("expected keys %v, got %v", expectedKeys, keys)
				}
			})

			t.Run("watch", func(t *testing.T) {
				watcher, err := kv.Watch(ctx, "watch.>", jetstream.UpdatesOnly())
				if err != nil {
					t.Fatalf("watch error: %v", err)
				}
				defer watcher.Stop()

				kv.Put(ctx, "watch.foo", []byte("foo value"))

				updates := watcher.Updates()

				select {
				case entry := <-updates:
					if entry == nil {
						t.Fatal("received unexpected nil entry")
					}
					expectedKey := "watch.foo"
					if tt.name == "path" {
						expectedKey = "watch/foo"
					}
					if entry.Key() != expectedKey {
						t.Errorf("expected key '%s', got %s", expectedKey, entry.Key())
					}
				case <-time.After(2 * time.Second):
					t.Fatal("timeout waiting for update")
				}

			})

			t.Run("additional methods", func(t *testing.T) {
				// Bucket
				bucket := kv.Bucket()
				if bucket == "" {
					t.Error("expected non-empty bucket name")
				}

				// PutString
				rev, err := kv.PutString(ctx, "string.key", "string value")
				if err != nil {
					t.Fatalf("PutString error: %v", err)
				}
				if rev == 0 {
					t.Error("expected non-zero revision")
				}

				// GetRevision
				entry, err := kv.GetRevision(ctx, "string.key", rev)
				if err != nil {
					t.Fatalf("GetRevision error: %v", err)
				}
				if entry.Key() != "string.key" {
					expectedKey := "string.key"
					if tt.name == "path" {
						expectedKey = "/string/key"
					}
					if entry.Key() != expectedKey {
						t.Errorf("expected key '%s', got %s", expectedKey, entry.Key())
					}
				}

				// Create
				rev2, err := kv.Create(ctx, "create.key", []byte("created value"))
				if err != nil {
					t.Fatalf("Create error: %v", err)
				}
				if rev2 == 0 {
					t.Error("expected non-zero revision")
				}

				// WatchAll
				watcher, err := kv.WatchAll(ctx)
				if err != nil {
					t.Fatalf("WatchAll error: %v", err)
				}
				watcher.Stop()

				// ListKeys
				lister, err := kv.ListKeys(ctx)
				if err != nil {
					t.Fatalf("ListKeys error: %v", err)
				}
				lister.Stop()

				// ListKeysFiltered
				pattern := "string.>"
				if tt.name == "path" {
					pattern = "/string/>"
				}
				lister2, err := kv.ListKeysFiltered(ctx, pattern)
				if err != nil {
					t.Fatalf("ListKeysFiltered error: %v", err)
				}
				lister2.Stop()

				// Status
				status, err := kv.Status(ctx)
				if err != nil {
					t.Fatalf("Status error: %v", err)
				}
				if status.Bucket() == "" {
					t.Error("expected non-empty bucket name in status")
				}

				// History
				history, err := kv.History(ctx, "string.key")
				if err != nil {
					t.Fatalf("History error: %v", err)
				}
				if len(history) == 0 {
					t.Error("expected non-empty history")
				}

				// PurgeDeletes
				err = kv.PurgeDeletes(ctx)
				if err != nil {
					t.Fatalf("PurgeDeletes error: %v", err)
				}

				// Purge
				err = kv.Purge(ctx, "string.key")
				if err != nil {
					t.Fatalf("Purge error: %v", err)
				}
			})
		})
	}
}

func TestPathCodec(t *testing.T) {
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

	// Create underlying KV
	baseKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "test_path",
	})
	if err != nil {
		t.Fatalf("create kv error: %v", err)
	}

	// path codec for keys
	pathCodec := kvcodec.PathCodec()
	kv := kvcodec.NewForKey(baseKV, pathCodec)

	_, err = kv.Put(ctx, "/config/app/database", []byte("postgres://localhost"))
	if err != nil {
		t.Fatalf("put error: %v", err)
	}

	// verify using baseKV directly
	// This should show the key as "config.app.database" without slashes
	rawEntry, err := baseKV.Get(ctx, "config.app.database")
	if err != nil {
		t.Fatalf("raw get error: %v", err)
	}
	if !bytes.Equal(rawEntry.Value(), []byte("postgres://localhost")) {
		t.Error("value mismatch")
	}

	// Get using path notation
	entry, err := kv.Get(ctx, "/config/app/database")
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	if entry.Key() != "/config/app/database" {
		t.Errorf("expected path key, got %s", entry.Key())
	}
}

func TestBase64Codec(t *testing.T) {
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

	t.Run("keys and values", func(t *testing.T) {
		baseKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket: "test_base64_both",
		})
		if err != nil {
			t.Fatalf("create kv error: %v", err)
		}

		// Base64 for both keys and values
		base64Codec := kvcodec.Base64Codec()
		kv := kvcodec.New(baseKV, base64Codec, base64Codec)

		originalKey := "Acme Inc.contact info"
		originalValue := []byte("email: info@acme.com, phone: +1-555-123")

		// Put using original key/value
		_, err = kv.Put(ctx, originalKey, originalValue)
		if err != nil {
			t.Fatalf("put error: %v", err)
		}

		// baseKV (sans codec) should show encoded keys/values
		keys, err := baseKV.Keys(ctx)
		if err != nil {
			t.Fatalf("keys error: %v", err)
		}
		if len(keys) != 1 {
			t.Fatalf("expected 1 key, got %d", len(keys))
		}

		// stored key should be base64 encoded
		encodedKey := keys[0]
		if encodedKey == originalKey {
			t.Error("stored key should be base64 encoded, not original")
		}

		// Get the raw entry to verify value is also encoded
		rawEntry, err := baseKV.Get(ctx, encodedKey)
		if err != nil {
			t.Fatalf("raw get error: %v", err)
		}

		// The stored value should be base64 encoded
		if bytes.Equal(rawEntry.Value(), originalValue) {
			t.Error("stored value should be base64 encoded, not original")
		}

		// Get using codec - should return original key/value
		entry, err := kv.Get(ctx, originalKey)
		if err != nil {
			t.Fatalf("get error: %v", err)
		}

		if entry.Key() != originalKey {
			t.Errorf("expected original key %q, got %q", originalKey, entry.Key())
		}

		if !bytes.Equal(entry.Value(), originalValue) {
			t.Errorf("expected original value %q, got %q", originalValue, entry.Value())
		}
	})

	t.Run("keys only", func(t *testing.T) {
		baseKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket: "test_base64_keys",
		})
		if err != nil {
			t.Fatalf("create kv error: %v", err)
		}

		// Base64 codec for keys only
		base64Codec := kvcodec.Base64Codec()
		noOpCodec := kvcodec.NoOpCodec()
		kv := kvcodec.New(baseKV, base64Codec, noOpCodec)

		originalKey := "Special Key with Spaces"
		originalValue := []byte("plain text value")

		_, err = kv.Put(ctx, originalKey, originalValue)
		if err != nil {
			t.Fatalf("put error: %v", err)
		}

		// Get raw entry to verify encoding behavior
		keys, err := baseKV.Keys(ctx)
		if err != nil {
			t.Fatalf("keys error: %v", err)
		}
		encodedKey := keys[0]
		rawEntry, err := baseKV.Get(ctx, encodedKey)
		if err != nil {
			t.Fatalf("raw get error: %v", err)
		}

		// Key should be encoded, value should be unchanged
		if encodedKey == originalKey {
			t.Error("key should be base64 encoded")
		}
		if !bytes.Equal(rawEntry.Value(), originalValue) {
			t.Error("value should be unchanged (no-op)")
		}

		// Codec access should return originals
		entry, err := kv.Get(ctx, originalKey)
		if err != nil {
			t.Fatalf("get error: %v", err)
		}
		if entry.Key() != originalKey {
			t.Errorf("expected original key %q, got %q", originalKey, entry.Key())
		}
		if !bytes.Equal(entry.Value(), originalValue) {
			t.Errorf("expected original value %q, got %q", originalValue, entry.Value())
		}
	})
}

func TestNoOpCodec(t *testing.T) {
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

	baseKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "test_noop",
	})
	if err != nil {
		t.Fatalf("create kv error: %v", err)
	}

	// Wrap with NoOpCodec for both keys and values
	noOpCodec := kvcodec.NoOpCodec()
	kv := kvcodec.New(baseKV, noOpCodec, noOpCodec)

	originalKey := "test.key"
	originalValue := []byte("test value")

	_, err = kv.Put(ctx, originalKey, originalValue)
	if err != nil {
		t.Fatalf("put error: %v", err)
	}

	// Verify underlying storage shows exact same data (no encoding)
	rawEntry, err := baseKV.Get(ctx, originalKey)
	if err != nil {
		t.Fatalf("raw get error: %v", err)
	}

	// Both key and value should be identical to originals
	if rawEntry.Key() != originalKey {
		t.Errorf("stored key should be unchanged: expected %q, got %q", originalKey, rawEntry.Key())
	}
	if !bytes.Equal(rawEntry.Value(), originalValue) {
		t.Errorf("stored value should be unchanged: expected %q, got %q", originalValue, rawEntry.Value())
	}

	// Get using codec should also return identical data
	entry, err := kv.Get(ctx, originalKey)
	if err != nil {
		t.Fatalf("get error: %v", err)
	}

	if entry.Key() != originalKey {
		t.Errorf("retrieved key should be unchanged: expected %q, got %q", originalKey, entry.Key())
	}
	if !bytes.Equal(entry.Value(), originalValue) {
		t.Errorf("retrieved value should be unchanged: expected %q, got %q", originalValue, entry.Value())
	}

	// Test that codec and direct access are identical
	directKeys, err := baseKV.Keys(ctx)
	if err != nil {
		t.Fatalf("direct keys error: %v", err)
	}
	codecKeys, err := kv.Keys(ctx)
	if err != nil {
		t.Fatalf("codec keys error: %v", err)
	}

	if len(directKeys) != len(codecKeys) {
		t.Errorf("key count mismatch: direct=%d, codec=%d", len(directKeys), len(codecKeys))
	}
	for i, directKey := range directKeys {
		if codecKeys[i] != directKey {
			t.Errorf("key mismatch at index %d: direct=%q, codec=%q", i, directKey, codecKeys[i])
		}
	}
}

// Test helper functions

func RunBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return RunServerWithOptions(&opts)
}

func shutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}

func RunServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}
