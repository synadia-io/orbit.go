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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Codec defines the interface for encoding and decoding values in the KV bucket.
type Codec interface {
	// Encode serializes any value to []byte
	Encode(value any) ([]byte, error)
	// DecodeBytes deserializes []byte to any type
	Decode(data []byte, target any) error
	// ValidType checks if the target type is supported by this codec
	ValidType(target any) bool
}

var (
	// NoOpCodec is a default codec that performs no encoding/decoding for []byte and string types.
	NoOpCodec = &noOpCodec{}

	// JSONCodec encodes and decodes JSON data.
	JSONCodec = &jsonCodec{}

	// SlashCodec is a codec that replaces '/' with '.' in strings and []byte.
	SlashCodec = &slashCodec{}
)

type jsonCodec struct{}

func (c *jsonCodec) Encode(value any) ([]byte, error) {
	return json.Marshal(value)
}

func (c *jsonCodec) Decode(data []byte, target any) error {
	return json.Unmarshal(data, target)
}

func (c *jsonCodec) ValidType(target any) bool {
	_, err := json.Marshal(target)
	return err == nil
}

type noOpCodec struct{}

func (c *noOpCodec) Encode(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("NoOpCodec only supports []byte and string types, got %T", value)
	}
}

func (c *noOpCodec) Decode(data []byte, target any) error {
	switch t := target.(type) {
	case *[]byte:
		*t = data
		return nil
	case *string:
		*t = string(data)
		return nil
	default:
		return fmt.Errorf("NoOpCodec only supports []byte and string types, requested %T", target)
	}
}

func (c *noOpCodec) ValidType(target any) bool {
	switch target.(type) {
	case []byte, string:
		return true
	default:
		return false
	}
}

type slashCodec struct{}

func (c *slashCodec) Encode(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		v = bytes.ReplaceAll(v, []byte{'/'}, []byte{'.'})
		return v, nil
	case string:
		v = strings.ReplaceAll(v, "/", ".")
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("SlashCodec only supports []byte and string types, got %T", value)
	}
}

func (c *slashCodec) Decode(data []byte, target any) error {
	switch t := target.(type) {
	case *[]byte:
		*t = bytes.ReplaceAll(data, []byte{'.'}, []byte{'/'})
		return nil
	case *string:
		*t = strings.ReplaceAll(string(data), ".", "/")
		return nil
	default:
		return fmt.Errorf("SlashCodec only supports []byte and string types, requested %T", target)
	}
}

func (c *slashCodec) ValidType(target any) bool {
	switch target.(type) {
	case []byte, string:
		return true
	default:
		return false
	}
}
