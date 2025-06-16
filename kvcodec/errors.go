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

import "errors"

// Codec-related errors
var (
	// ErrNoCodecs is returned when creating a ChainCodec with no codecs.
	ErrNoCodecs = errors.New("at least one codec must be provided")

	// ErrWildcardNotSupported is returned when a non-FilterableCodec is used to encode keys with wildcards.
	ErrWildcardNotSupported = errors.New("codec does not support wildcard filtering")

	// ErrKeyEncodeFailed is returned when a codec fails to encode a key.
	ErrKeyEncodeFailed = errors.New("failed to encode key")

	// ErrKeyDecodeFailed is returned when a codec fails to decode a key.
	ErrKeyDecodeFailed = errors.New("failed to decode key")

	// ErrValueEncodeFailed is returned when a codec fails to encode a value.
	ErrValueEncodeFailed = errors.New("failed to encode value")

	// ErrValueDecodeFailed is returned when a codec fails to decode a value.
	ErrValueDecodeFailed = errors.New("failed to decode value")

	// ErrFilterEncodeFailed is returned when a codec fails to encode a filter pattern.
	ErrFilterEncodeFailed = errors.New("failed to encode filter pattern")
)
