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

package kv

import (
	"fmt"
	"time"
)

// IncludeHistory instructs the key watcher to include historical values as
// well (up to KeyValueMaxHistory).
func IncludeHistory() WatchOpt {
	return func(opts *watchOpts) error {
		if opts.updatesOnly {
			return fmt.Errorf("%w: include history can not be used with updates only", ErrInvalidOption)
		}
		opts.includeHistory = true
		return nil
	}
}

// UpdatesOnly instructs the key watcher to only include updates on values
// (without latest values when started).
func UpdatesOnly() WatchOpt {
	return func(opts *watchOpts) error {
		if opts.includeHistory {
			return fmt.Errorf("%w: updates only can not be used with include history", ErrInvalidOption)
		}
		opts.updatesOnly = true
		return nil
	}
}

// IgnoreDeletes will have the key watcher not pass any deleted keys.
func IgnoreDeletes() WatchOpt {
	return func(opts *watchOpts) error {
		opts.ignoreDeletes = true
		return nil
	}
}

// MetaOnly instructs the key watcher to retrieve only the entry meta data, not
// the entry value.
func MetaOnly() WatchOpt {
	return func(opts *watchOpts) error {
		opts.metaOnly = true
		return nil
	}
}

// ResumeFromRevision instructs the key watcher to resume from a specific
// revision number.
func ResumeFromRevision(revision uint64) WatchOpt {
	return func(opts *watchOpts) error {
		opts.resumeFromRevision = revision
		return nil
	}
}

// DeleteMarkersOlderThan indicates that delete or purge markers older than that
// will be deleted as part of [KeyValue.PurgeDeletes] operation, otherwise, only the data
// will be removed but markers that are recent will be kept.
// Note that if no option is specified, the default is 30 minutes. You can set
// this option to a negative value to instruct to always remove the markers,
// regardless of their age.
func DeleteMarkersOlderThan(dur time.Duration) KVPurgeDeletesOpt {
	return func(opts *purgeDeletesOpts) error {
		opts.dmthr = dur
		return nil
	}
}

// DeleteLastRevision deletes if the latest revision matches the provided one. If the
// provided revision is not the latest, delete will return an error.
func DeleteLastRevision(revision uint64) KVDeleteOpt {
	return func(opts *deleteOpts) error {
		opts.revision = revision
		return nil
	}
}

// PurgeTTL sets the TTL for the purge operation.
// After the TTL expires, the delete markers will be removed.
// This requires LimitMarkerTTL to be enabled on the bucket.
// Note that this is not the same as the TTL for the key itself, which is set
// using the KeyTTL option when creating the key.
func PurgeTTL(ttl time.Duration) KVPurgeOpt {
	return func(opts *purgeOpts) error {
		opts.ttl = ttl
		return nil
	}
}

// PurgeLastRevision purges if the latest revision matches the provided one. If the
// provided revision is not the latest, purge will return an error.
func PurgeLastRevision(revision uint64) KVPurgeOpt {
	return func(opts *purgeOpts) error {
		opts.revision = revision
		return nil
	}
}

// KeyTTL sets the TTL for the key. This is the time after which the key will be
// automatically deleted. The TTL is set when the key is created and can not be
// changed later. This requires LimitMarkerTTL to be enabled on the bucket.
func KeyTTL(ttl time.Duration) KVCreateOpt {
	return func(opts *createOpts) error {
		opts.ttl = ttl
		return nil
	}
}

// WithKeyCodec sets the codec to use for encoding and decoding keys in the key
// value store. This is used to serialize and deserialize keys when storing
// and retrieving them from the store. The codec must implement the Codec
// interface.
func WithKeyCodec(codec Codec) GetKeyValueOpt {
	return func(opts *getKeyValueOpts) error {
		if codec == nil {
			return fmt.Errorf("%w: codec can not be nil", ErrInvalidOption)
		}
		opts.KeyCodec = codec
		return nil
	}
}

// WithValueCodec sets the codec to use for encoding and decoding values in the key
// value store. This is used to serialize and deserialize values when storing
// and retrieving them from the store. The codec must implement the Codec
// interface.
func WithValueCodec(codec Codec) GetKeyValueOpt {
	return func(opts *getKeyValueOpts) error {
		if codec == nil {
			return fmt.Errorf("%w: codec can not be nil", ErrInvalidOption)
		}
		opts.ValueCodec = codec
		return nil
	}
}
