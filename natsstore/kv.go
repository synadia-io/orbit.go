package natsstore

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
)

func NewKeyValueStore(js jetstream.JetStream, bucket string, codec Codec) (Store, error) {
	kv, err := js.KeyValue(context.Background(), bucket)
	if err != nil {
		return nil, err
	}

	stream, err := js.Stream(context.Background(), fmt.Sprintf("KV_%s", bucket))
	if err != nil {
		return nil, err
	}

	return &KeyValueStore{
		stream: stream,
		kv:     kv,
		codec:  codec,
	}, nil
}

type KeyValueStore struct {
	stream jetstream.Stream
	kv     jetstream.KeyValue
	codec  Codec
}

func (s *KeyValueStore) Codec() Codec {
	return s.codec
}

func (s *KeyValueStore) List(ctx context.Context, filter string, handler EntryHandler) error {
	watcher, err := s.kv.Watch(ctx, filter, jetstream.IgnoreDeletes())
	if err != nil {
		return err
	}
	defer watcher.Stop()

	for v := range watcher.Updates() {
		if v == nil {
			break
		}

		entry := newEntry(s.codec)
		entry.Revision = v.Revision()
		entry.Data = v.Value()
		entry.Key = v.Key()

		if err := handler(entry, true); err != nil {
			return err
		}
	}

	return handler(newEntry(s.codec), false)
}

func (s *KeyValueStore) Apply(ctx context.Context, key string, mutator Mutator) (uint64, error) {
	for {
		ekve, err := s.kv.Get(ctx, key)
		if err != nil {
			if !errors.Is(err, jetstream.ErrKeyNotFound) {
				return 0, err
			}

			ekve = nil
		}

		entry := newEntry(s.codec)
		if ekve != nil {
			entry.Key = ekve.Key()
			entry.Data = ekve.Value()
			entry.Revision = ekve.Revision()
		}

		res, err := mutator(entry)
		// in case of an error during execution of the mutator, we will not retry but return the error instead
		if err != nil {
			return 0, err
		}

		// -- if the result is nul, we will delete the entry from the kv store
		if res == nil && entry.Revision != 0 {
			return entry.Revision, s.kv.Delete(ctx, key)
		}

		// -- if we did get a result, we will need to persist the entry
		// -- if the revision was 0, we will need to create a new entry
		if entry.Revision == 0 {
			rev, err := s.kv.Create(ctx, key, res)
			if err != nil {
				if errors.Is(err, jetstream.ErrKeyExists) {
					continue
				}

				return 0, err
			}
			return rev, nil
		} else {
			rev, err := s.kv.Update(ctx, key, res, entry.Revision)
			if err != nil {
				if errors.Is(err, jetstream.ErrKeyExists) {
					continue
				}

				return 0, err
			}
			return rev, nil
		}
	}
}

func (s *KeyValueStore) Exists(ctx context.Context, key string) (bool, error) {
	entry, err := s.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return false, nil
		}

		return false, err
	}

	return entry.Operation() == jetstream.KeyValuePut, nil
}

func (s *KeyValueStore) Get(ctx context.Context, key string) (*Entry, bool, error) {
	entry := newEntry(s.codec)
	ekve, err := s.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, false, nil
		}

		return nil, false, err
	}

	entry.Key = ekve.Key()
	entry.Data = ekve.Value()
	entry.Revision = ekve.Revision()

	return &entry, true, nil
}

func (s *KeyValueStore) Delete(ctx context.Context, key string) error {
	return s.kv.Delete(ctx, key)
}

func (s *KeyValueStore) Purge(ctx context.Context, key string) error {
	kvKey := fmt.Sprintf("$KV.%s.%s", s.kv.Bucket(), key)
	return s.stream.Purge(ctx, jetstream.WithPurgeSubject(kvKey))
}
