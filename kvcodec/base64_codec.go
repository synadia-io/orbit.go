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
	"encoding/base64"
	"fmt"
	"strings"
)

type base64Codec struct{}

// Base64Codec returns a codec that encodes keys and values using base64 encoding.
func Base64Codec() *base64Codec {
	return &base64Codec{}
}

// EncodeKey encodes each token of the key separately.
func (b *base64Codec) EncodeKey(key string) (string, error) {
	tokens := strings.Split(key, ".")

	for i, token := range tokens {
		tokens[i] = base64.RawURLEncoding.EncodeToString([]byte(token))
	}

	return strings.Join(tokens, "."), nil
}

// DecodeKey decodes each token of the key.
func (b *base64Codec) DecodeKey(key string) (string, error) {
	tokens := strings.Split(key, ".")

	for i, token := range tokens {
		decoded, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			return "", fmt.Errorf("failed to decode base64 token: %w", err)
		}
		tokens[i] = string(decoded)
	}

	return strings.Join(tokens, "."), nil
}

// EncodeValue encodes the value using base64.
func (b *base64Codec) EncodeValue(value []byte) ([]byte, error) {
	encoded := make([]byte, base64.RawURLEncoding.EncodedLen(len(value)))
	base64.RawURLEncoding.Encode(encoded, value)
	return encoded, nil
}

// DecodeValue decodes the value from base64.
func (b *base64Codec) DecodeValue(value []byte) ([]byte, error) {
	decoded := make([]byte, base64.RawURLEncoding.DecodedLen(len(value)))
	n, err := base64.RawURLEncoding.Decode(decoded, value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 value: %w", err)
	}

	return decoded[:n], nil
}

// EncodeFilter implements FilterableKeyCodec for Base64Codec.
// It encodes non-wildcard tokens while preserving * and > wildcards.
func (b *base64Codec) EncodeFilter(filter string) (string, error) {
	tokens := strings.Split(filter, ".")
	encodedTokens := make([]string, len(tokens))

	for i, token := range tokens {
		if token == ">" || token == "*" {
			encodedTokens[i] = token
		} else {
			encodedTokens[i] = base64.RawURLEncoding.EncodeToString([]byte(token))
		}
	}

	return strings.Join(encodedTokens, "."), nil
}
