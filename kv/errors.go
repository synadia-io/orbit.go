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
)

type (
	// KVError is an error type returned by the KV API.
	KVError struct {
		message string
	}
)

var (
	// ErrKeyExists is returned when attempting to create a key that already
	// exists.
	ErrKeyExists = &KVError{message: "key exists"}

	// ErrKeyValueConfigRequired is returned when attempting to create a bucket
	// without a config.
	ErrKeyValueConfigRequired = &KVError{message: "config required"}

	// ErrInvalidBucketName is returned when attempting to create a bucket with
	// an invalid name.
	ErrInvalidBucketName = &KVError{message: "invalid bucket name"}

	// ErrInvalidKey is returned when attempting to create a key with an invalid
	// name.
	ErrInvalidKey = &KVError{message: "invalid key"}

	// ErrBucketExists is returned when attempting to create a bucket that
	// already exists and has a different configuration.
	ErrBucketExists = &KVError{message: "bucket name already in use"}

	// ErrBucketNotFound is returned when attempting to access a bucket that
	// does not exist.
	ErrBucketNotFound = &KVError{message: "bucket not found"}

	// ErrBadBucket is returned when attempting to access a bucket that is not a
	// key-value store.
	ErrBadBucket = &KVError{message: "bucket not valid key-value store"}

	// ErrKeyNotFound is returned when attempting to access a key that does not
	// exist.
	ErrKeyNotFound = &KVError{message: "key not found"}

	// ErrKeyDeleted is returned when attempting to access a key that was
	// deleted.
	ErrKeyDeleted = &KVError{message: "key was deleted"}

	// ErrHistoryToLarge is returned when provided history limit is larger than
	// 64.
	ErrHistoryTooLarge = &KVError{message: "history limited to a max of 64"}

	// ErrNoKeysFound is returned when no keys are found.
	ErrNoKeysFound = &KVError{message: "no keys found"}

	// ErrTTLOnDeleteNotSupported is returned when attempting to set a TTL
	// on a delete operation.
	ErrTTLOnDeleteNotSupported = &KVError{message: "TTL is not supported on delete"}

	// ErrLimitMarkerTTLNotSupported is returned when the connected jetstream API
	// does not support setting the LimitMarkerTTL.
	ErrLimitMarkerTTLNotSupported = &KVError{message: "limit marker TTLs not supported by server"}

	// ErrInvalidOption is returned when there is a collision between options.
	ErrInvalidOption = &KVError{message: "invalid option"}
)

func (err *KVError) Error() string {
	return fmt.Sprintf("nats: %s", err.message)
}
