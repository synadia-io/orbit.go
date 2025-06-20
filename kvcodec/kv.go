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

package kvcodec

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type (
	// keyValue wraps jetstream.KeyValue with codec capabilities.
	keyValue struct {
		kv         jetstream.KeyValue
		keyCodec   KeyCodec
		valueCodec ValueCodec
	}

	// kvWatcher wraps jetstream.KeyWatcher with codec capabilities.
	kvWatcher struct {
		watcher    jetstream.KeyWatcher
		keyCodec   KeyCodec
		valueCodec ValueCodec
		updatesCh  chan jetstream.KeyValueEntry
	}

	// kvLister wraps jetstream.KeyLister to provide codec functionality.
	kvLister struct {
		lister   jetstream.KeyLister
		keyCodec KeyCodec
	}

	// kvEntry wraps jetstream.KeyValueEntry to provide codec functionality.
	kvEntry struct {
		jetstream.KeyValueEntry
		keyCodec    KeyCodec
		valueCodec  ValueCodec
		originalKey string
	}
)

// New creates a new codec-enabled KeyValue wrapper with separate key and value codecs.
func New(kv jetstream.KeyValue, keyCodec KeyCodec, valueCodec ValueCodec) jetstream.KeyValue {
	if keyCodec == nil {
		keyCodec = NoOpCodec()
	}
	if valueCodec == nil {
		valueCodec = NoOpCodec()
	}
	return &keyValue{
		kv:         kv,
		keyCodec:   keyCodec,
		valueCodec: valueCodec,
	}
}

// NewForKey creates a new KeyValue wrapper with only key encoding enabled.
// Values will use NoOpCodec (no encoding).
func NewForKey(kv jetstream.KeyValue, keyCodec KeyCodec) jetstream.KeyValue {
	return New(kv, keyCodec, NoOpCodec())
}

// NewForValue creates a new KeyValue wrapper with only value encoding enabled.
// Keys will use NoOpCodec (no encoding).
func NewForValue(kv jetstream.KeyValue, valueCodec ValueCodec) jetstream.KeyValue {
	return New(kv, NoOpCodec(), valueCodec)
}

// Bucket returns the bucket name.
func (c *keyValue) Bucket() string {
	return c.kv.Bucket()
}

// Put puts a key-value pair into the store.
func (c *keyValue) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	encodedKey, err := c.keyCodec.EncodeKey(key)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrKeyEncodeFailed, err)
	}

	encodedValue, err := c.valueCodec.EncodeValue(value)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrValueEncodeFailed, err)
	}

	return c.kv.Put(ctx, encodedKey, encodedValue)
}

// PutString puts a string value into the store.
func (c *keyValue) PutString(ctx context.Context, key string, value string) (uint64, error) {
	return c.Put(ctx, key, []byte(value))
}

// Get retrieves a value from the store.
func (c *keyValue) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	encodedKey, err := c.keyCodec.EncodeKey(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrKeyEncodeFailed, err)
	}

	entry, err := c.kv.Get(ctx, encodedKey)
	if err != nil {
		return nil, err
	}

	return &kvEntry{
		KeyValueEntry: entry,
		keyCodec:      c.keyCodec,
		valueCodec:    c.valueCodec,
		originalKey:   key,
	}, nil
}

// GetRevision retrieves a specific revision of a key.
func (c *keyValue) GetRevision(ctx context.Context, key string, revision uint64) (jetstream.KeyValueEntry, error) {
	encodedKey, err := c.keyCodec.EncodeKey(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrKeyEncodeFailed, err)
	}

	entry, err := c.kv.GetRevision(ctx, encodedKey, revision)
	if err != nil {
		return nil, err
	}

	return &kvEntry{
		KeyValueEntry: entry,
		keyCodec:      c.keyCodec,
		valueCodec:    c.valueCodec,
		originalKey:   key,
	}, nil
}

// Create creates a new key-value pair, failing if it already exists.
func (c *keyValue) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	encodedKey, err := c.keyCodec.EncodeKey(key)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrKeyEncodeFailed, err)
	}

	encodedValue, err := c.valueCodec.EncodeValue(value)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrValueEncodeFailed, err)
	}

	return c.kv.Create(ctx, encodedKey, encodedValue, opts...)
}

// Update updates an existing key-value pair.
func (c *keyValue) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	encodedKey, err := c.keyCodec.EncodeKey(key)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrKeyEncodeFailed, err)
	}

	encodedValue, err := c.valueCodec.EncodeValue(value)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrValueEncodeFailed, err)
	}

	return c.kv.Update(ctx, encodedKey, encodedValue, revision)
}

// Delete deletes a key from the store.
func (c *keyValue) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	encodedKey, err := c.keyCodec.EncodeKey(key)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrKeyEncodeFailed, err)
	}

	return c.kv.Delete(ctx, encodedKey, opts...)
}

// Purge removes all revisions of a key.
func (c *keyValue) Purge(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	encodedKey, err := c.keyCodec.EncodeKey(key)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrKeyEncodeFailed, err)
	}

	return c.kv.Purge(ctx, encodedKey, opts...)
}

// WatchFiltered creates a watcher for the specified keys (may contain wildcards).
func (c *keyValue) WatchFiltered(ctx context.Context, keys []string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	encodedKeys := make([]string, len(keys))

	// check if codec supports filtering
	if fc, ok := c.keyCodec.(FilterableKeyCodec); ok {
		// use EncodeFilter for each key pattern
		for i, key := range keys {
			encoded, err := fc.EncodeFilter(key)
			if err != nil {
				return nil, fmt.Errorf("%w %q: %w", ErrFilterEncodeFailed, key, err)
			}
			encodedKeys[i] = encoded
		}
	} else {
		// fallback to EncodeKey, but check for wildcards
		for i, key := range keys {
			if strings.Contains(key, "*") || strings.Contains(key, ">") {
				return nil, fmt.Errorf("%w in key %q; use WatchAll() and filter client-side instead", ErrWildcardNotSupported, key)
			}
			encoded, err := c.keyCodec.EncodeKey(key)
			if err != nil {
				return nil, fmt.Errorf("%w %q: %w", ErrKeyEncodeFailed, key, err)
			}
			encodedKeys[i] = encoded
		}
	}

	watcher, err := c.kv.WatchFiltered(ctx, encodedKeys, opts...)
	if err != nil {
		return nil, err
	}

	return &kvWatcher{
		watcher:    watcher,
		keyCodec:   c.keyCodec,
		valueCodec: c.valueCodec,
	}, nil
}

// Watch creates a watcher for the specified keys (may contain wildcards).
func (c *keyValue) Watch(ctx context.Context, keys string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return c.WatchFiltered(ctx, []string{keys}, opts...)
}

// WatchAll watches all keys in the store.
func (c *keyValue) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	watcher, err := c.kv.WatchAll(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &kvWatcher{
		watcher:    watcher,
		keyCodec:   c.keyCodec,
		valueCodec: c.valueCodec,
	}, nil
}

// Keys returns all keys in the store.
func (c *keyValue) Keys(ctx context.Context, opts ...jetstream.WatchOpt) ([]string, error) {
	encodedKeys, err := c.kv.Keys(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// Decode keys
	keys := make([]string, len(encodedKeys))
	for i, encodedKey := range encodedKeys {
		decodedKey, err := c.keyCodec.DecodeKey(encodedKey)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrKeyDecodeFailed, err)
		}
		keys[i] = decodedKey
	}

	return keys, nil
}

// History retrieves the history of a key.
func (c *keyValue) History(ctx context.Context, key string, opts ...jetstream.WatchOpt) ([]jetstream.KeyValueEntry, error) {
	encodedKey, err := c.keyCodec.EncodeKey(key)
	if err != nil {
		return nil, err
	}

	entries, err := c.kv.History(ctx, encodedKey, opts...)
	if err != nil {
		return nil, err
	}

	// Wrap entries with codec
	result := make([]jetstream.KeyValueEntry, len(entries))
	for i, entry := range entries {
		result[i] = &kvEntry{
			KeyValueEntry: entry,
			keyCodec:      c.keyCodec,
			valueCodec:    c.valueCodec,
			originalKey:   key,
		}
	}

	return result, nil
}

// PurgeDeletes purges all deleted keys.
func (c *keyValue) PurgeDeletes(ctx context.Context, opts ...jetstream.KVPurgeOpt) error {
	return c.kv.PurgeDeletes(ctx, opts...)
}

// Status returns the status of the KeyValue store.
func (c *keyValue) Status(ctx context.Context) (jetstream.KeyValueStatus, error) {
	return c.kv.Status(ctx)
}

// ListKeys returns a KeyLister for streaming all keys.
func (c *keyValue) ListKeys(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyLister, error) {
	lister, err := c.kv.ListKeys(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &kvLister{
		lister:   lister,
		keyCodec: c.keyCodec,
	}, nil
}

// ListKeysFiltered returns a KeyLister for filtered keys.
func (c *keyValue) ListKeysFiltered(ctx context.Context, filters ...string) (jetstream.KeyLister, error) {
	encodedFilters := make([]string, len(filters))

	// Check if codec supports filtering
	if fc, ok := c.keyCodec.(FilterableKeyCodec); ok {
		// Use EncodeFilter for each filter pattern
		for i, filter := range filters {
			encoded, err := fc.EncodeFilter(filter)
			if err != nil {
				return nil, fmt.Errorf("%w %q: %w", ErrFilterEncodeFailed, filter, err)
			}
			encodedFilters[i] = encoded
		}
	} else {
		// Fallback to EncodeKey, but check for wildcards
		for i, filter := range filters {
			if strings.Contains(filter, "*") || strings.Contains(filter, ">") {
				return nil, ErrWildcardNotSupported
			}
			encoded, err := c.keyCodec.EncodeKey(filter)
			if err != nil {
				return nil, fmt.Errorf("%w %q: %w", ErrFilterEncodeFailed, filter, err)
			}
			encodedFilters[i] = encoded
		}
	}

	lister, err := c.kv.ListKeysFiltered(ctx, encodedFilters...)
	if err != nil {
		return nil, err
	}

	return &kvLister{
		lister:   lister,
		keyCodec: c.keyCodec,
	}, nil
}

// Updates returns a channel of decoded key value entries.
func (w *kvWatcher) Updates() <-chan jetstream.KeyValueEntry {
	if w.updatesCh != nil {
		return w.updatesCh
	}
	w.updatesCh = make(chan jetstream.KeyValueEntry, 256)

	go func() {
		defer close(w.updatesCh)

		for entry := range w.watcher.Updates() {
			if entry == nil {
				w.updatesCh <- nil
				continue
			}

			// Wrap the entry with codec
			w.updatesCh <- &kvEntry{
				KeyValueEntry: entry,
				keyCodec:      w.keyCodec,
				valueCodec:    w.valueCodec,
			}
		}
	}()

	return w.updatesCh
}

// Stop stops the watcher.
func (w *kvWatcher) Stop() error {
	return w.watcher.Stop()
}

// Keys returns a channel of decoded keys.
func (l *kvLister) Keys() <-chan string {
	// Create a new channel for decoded keys
	decodedCh := make(chan string)

	go func() {
		defer close(decodedCh)

		for encodedKey := range l.lister.Keys() {
			decodedKey, err := l.keyCodec.DecodeKey(encodedKey)
			if err != nil {
				// If decoding fails, skip the key
				continue
			}
			decodedCh <- decodedKey
		}
	}()

	return decodedCh
}

// Stop stops the key lister.
func (l *kvLister) Stop() error {
	return l.lister.Stop()
}

// Key returns the decoded key.
func (e *kvEntry) Key() string {
	if e.originalKey != "" {
		return e.originalKey
	}

	decodedKey, err := e.keyCodec.DecodeKey(e.KeyValueEntry.Key())
	if err != nil {
		// return encoded key if decoding fails
		return e.KeyValueEntry.Key()
	}
	return decodedKey
}

// Value returns the decoded value.
func (e *kvEntry) Value() []byte {
	val := e.KeyValueEntry.Value()
	if val == nil {
		return nil
	}

	decodedValue, err := e.valueCodec.DecodeValue(val)
	if err != nil {
		// return encoded value if decoding fails
		return val
	}
	return decodedValue
}

// Revision returns the revision number.
func (e *kvEntry) Revision() uint64 {
	return e.KeyValueEntry.Revision()
}

// Created returns the creation time.
func (e *kvEntry) Created() time.Time {
	return e.KeyValueEntry.Created()
}

// Delta is distance from the latest value (how far the current sequence
// is from the latest).
func (e *kvEntry) Delta() uint64 {
	return e.KeyValueEntry.Delta()
}

// Operation returns the operation type.
func (e *kvEntry) Operation() jetstream.KeyValueOp {
	return e.KeyValueEntry.Operation()
}

// Bucket returns the bucket name.
func (e *kvEntry) Bucket() string {
	return e.KeyValueEntry.Bucket()
}
