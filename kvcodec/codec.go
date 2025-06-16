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

// KeyCodec defines the interface for encoding/decoding keys in a KV bucket.
type KeyCodec interface {
	// EncodeKey encodes a key for storage.
	EncodeKey(key string) (string, error)

	// DecodeKey decodes a key retrieved from storage.
	DecodeKey(key string) (string, error)
}

// ValueCodec defines the interface for encoding/decoding values in a KV bucket.
type ValueCodec interface {
	// EncodeValue encodes a value for storage.
	EncodeValue(value []byte) ([]byte, error)

	// DecodeValue decodes a value retrieved from storage.
	DecodeValue(value []byte) ([]byte, error)
}

// FilterableKeyCodec is an optional interface that key codecs can implement
// to support wildcard filtering operations like Watch(ctx, "foo.*").
// If a key codec doesn't implement this interface, filter operations where
// the pattern contains wildcards will fail with ErrWildcardNotSupported.
type FilterableKeyCodec interface {
	KeyCodec
	// EncodeFilter encodes a pattern that may contain wildcards (* or >).
	// Unlike EncodeKey, this must preserve wildcards in the result.
	EncodeFilter(filter string) (string, error)
}

// noOpCodec is a no-op codec that passes data through unchanged.
// It implements both KeyCodec and ValueCodec interfaces.
type noOpCodec struct{}

// NoOpCodec returns a codec that passes data through unchanged.
// It implements both KeyCodec and ValueCodec interfaces.
func NoOpCodec() *noOpCodec {
	return &noOpCodec{}
}

func (p *noOpCodec) EncodeKey(key string) (string, error) {
	return key, nil
}

func (p *noOpCodec) DecodeKey(key string) (string, error) {
	return key, nil
}

func (p *noOpCodec) EncodeValue(value []byte) ([]byte, error) {
	return value, nil
}

func (p *noOpCodec) DecodeValue(value []byte) ([]byte, error) {
	return value, nil
}

// EncodeFilter implements FilterableKeyCodec for noOpCodec.
func (p *noOpCodec) EncodeFilter(filter string) (string, error) {
	return filter, nil
}
