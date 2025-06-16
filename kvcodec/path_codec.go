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

import "strings"

type pathCodec struct{}

// PathCodec returns a codec that translates between path-style keys (using /)
// and NATS subjects notation (using .).
func PathCodec() *pathCodec {
	return &pathCodec{}
}

// EncodeKey converts path-style key to NATS-style key.
func (p *pathCodec) EncodeKey(key string) (string, error) {
	// trim / from the beginning and end of the key
	// as subjects do not allow leading or trailing .
	key = strings.Trim(key, "/")

	// Replace / with .
	return strings.ReplaceAll(key, "/", "."), nil
}

// DecodeKey converts NATS-style key to path-style key.
func (p *pathCodec) DecodeKey(key string) (string, error) {
	return strings.ReplaceAll(key, ".", "/"), nil
}

// EncodeValue passes through the value unchanged.
func (p *pathCodec) EncodeValue(value []byte) ([]byte, error) {
	return value, nil
}

// DecodeValue passes through the value unchanged.
func (p *pathCodec) DecodeValue(value []byte) ([]byte, error) {
	return value, nil
}

// EncodeFilter implements FilterableKeyCodec for PathCodec.
func (p *pathCodec) EncodeFilter(filter string) (string, error) {
	return p.EncodeKey(filter)
}
