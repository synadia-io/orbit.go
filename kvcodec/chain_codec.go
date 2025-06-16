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

import "fmt"

// KeyChainCodec applies multiple key codecs in sequence.
// Encoding is applied in order (first to last), decoding in reverse order (last to first).
type KeyChainCodec struct {
	codecs []KeyCodec
}

// ValueChainCodec applies multiple value codecs in sequence.
// Encoding is applied in order (first to last), decoding in reverse order (last to first).
type ValueChainCodec struct {
	codecs []ValueCodec
}

// NewKeyChainCodec creates a new KeyChainCodec that applies the given key codecs in sequence.
// At least one codec must be provided.
func NewKeyChainCodec(codecs ...KeyCodec) (KeyCodec, error) {
	if len(codecs) == 0 {
		return nil, ErrNoCodecs
	}
	return &KeyChainCodec{codecs: codecs}, nil
}

// NewValueChainCodec creates a new ValueChainCodec that applies the given value codecs in sequence.
// At least one codec must be provided.
func NewValueChainCodec(codecs ...ValueCodec) (ValueCodec, error) {
	if len(codecs) == 0 {
		return nil, ErrNoCodecs
	}
	return &ValueChainCodec{codecs: codecs}, nil
}

// EncodeKey applies all codecs to the key in sequence (first to last).
func (c *KeyChainCodec) EncodeKey(key string) (string, error) {
	result := key
	for i, codec := range c.codecs {
		var err error
		result, err = codec.EncodeKey(result)
		if err != nil {
			return "", fmt.Errorf("%w (codec %d): %w", ErrKeyEncodeFailed, i, err)
		}
	}
	return result, nil
}

// DecodeKey applies all codecs to the key in reverse order (last to first).
func (c *KeyChainCodec) DecodeKey(key string) (string, error) {
	result := key
	for i := len(c.codecs) - 1; i >= 0; i-- {
		var err error
		result, err = c.codecs[i].DecodeKey(result)
		if err != nil {
			return "", fmt.Errorf("%w (codec %d): %w", ErrKeyDecodeFailed, i, err)
		}
	}
	return result, nil
}

// EncodeValue applies all codecs to the value in sequence (first to last).
func (c *ValueChainCodec) EncodeValue(value []byte) ([]byte, error) {
	result := value
	for i, codec := range c.codecs {
		var err error
		result, err = codec.EncodeValue(result)
		if err != nil {
			return nil, fmt.Errorf("%w (codec %d): %w", ErrValueEncodeFailed, i, err)
		}
	}
	return result, nil
}

// DecodeValue applies all codecs to the value in reverse order (last to first).
func (c *ValueChainCodec) DecodeValue(value []byte) ([]byte, error) {
	result := value
	for i := len(c.codecs) - 1; i >= 0; i-- {
		var err error
		result, err = c.codecs[i].DecodeValue(result)
		if err != nil {
			return nil, fmt.Errorf("%w (codec %d): %w", ErrValueDecodeFailed, i, err)
		}
	}
	return result, nil
}

// EncodeFilter implements FilterableKeyCodec only if ALL codecs in the chain support it.
// For wildcard patterns to work correctly, every codec in the chain must preserve wildcards.
func (c *KeyChainCodec) EncodeFilter(filter string) (string, error) {
	// Check if ALL codecs support filtering - if any doesn't, we can't guarantee wildcard preservation
	for i, codec := range c.codecs {
		if _, ok := codec.(FilterableKeyCodec); !ok {
			return "", fmt.Errorf("%w: codec %d does not implement FilterableKeyCodec", ErrWildcardNotSupported, i)
		}
	}

	// All codecs support filtering, apply them in sequence
	result := filter
	for i, codec := range c.codecs {
		filterable := codec.(FilterableKeyCodec) // Safe cast since we checked above
		var err error
		result, err = filterable.EncodeFilter(result)
		if err != nil {
			return "", fmt.Errorf("%w (codec %d): %w", ErrFilterEncodeFailed, i, err)
		}
	}
	return result, nil
}
