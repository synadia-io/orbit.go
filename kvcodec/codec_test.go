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
	"bytes"
	"testing"
)

func TestNoOpCodec(t *testing.T) {
	codec := NoOpCodec()

	t.Run("keys", func(t *testing.T) {
		key := "foo.bar.baz"
		encoded, err := codec.EncodeKey(key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if encoded != key {
			t.Errorf("expected %s, got %s", key, encoded)
		}
		decoded, err := codec.DecodeKey(key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decoded != key {
			t.Errorf("expected %s, got %s", key, decoded)
		}
	})

	t.Run("values", func(t *testing.T) {
		value := []byte("test value")
		encoded, err := codec.EncodeValue(value)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(encoded, value) {
			t.Errorf("expected %s, got %s", value, encoded)
		}
		decoded, err := codec.DecodeValue(value)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(decoded, value) {
			t.Errorf("expected %s, got %s", value, decoded)
		}
	})
}

func TestBase64Codec(t *testing.T) {
	codec := Base64Codec()

	t.Run("keys", func(t *testing.T) {
		key := "test.key.with.special.chars!@#$%^&*()"
		expected := "dGVzdA.a2V5.d2l0aA.c3BlY2lhbA.Y2hhcnMhQCMkJV4mKigp"
		encoded, err := codec.EncodeKey(key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if encoded != expected {
			t.Errorf("encode: expected %s, got %s", expected, encoded)
		}

		decoded, err := codec.DecodeKey(encoded)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decoded != key {
			t.Errorf("decode: expected %s, got %s", key, decoded)
		}
	})

	t.Run("values", func(t *testing.T) {
		value := []byte("test value with special chars: !@#$%^&*()")

		encoded, err := codec.EncodeValue(value)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decoded, err := codec.DecodeValue(encoded)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !bytes.Equal(decoded, value) {
			t.Errorf("expected %s, got %s", value, decoded)
		}
	})
}

func TestPathCodec(t *testing.T) {
	codec := PathCodec()

	t.Run("keys", func(t *testing.T) {
		tests := []struct {
			name    string
			input   string
			encoded string
			decoded string
		}{
			{"simple path", "/foo/bar", "_root_.foo.bar", "/foo/bar"},
			{"no leading slash", "foo/bar", "foo.bar", "foo/bar"},
			{"deep path", "/foo/bar/baz/qux", "_root_.foo.bar.baz.qux", "/foo/bar/baz/qux"},
			{"single segment", "/foo", "_root_.foo", "/foo"},
			{"trailing slash", "foo/bar/", "foo.bar", "foo/bar"},
			{"root only", "/", "_root_", "/"},
			{"leading slash with trailing", "/foo/bar/", "_root_.foo.bar", "/foo/bar"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				encoded, err := codec.EncodeKey(tt.input)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if encoded != tt.encoded {
					t.Errorf("encode: expected %s, got %s", tt.encoded, encoded)
				}

				decoded, err := codec.DecodeKey(encoded)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if decoded != tt.decoded {
					t.Errorf("decode: expected %s, got %s", tt.decoded, decoded)
				}
			})
		}
	})

	t.Run("values", func(t *testing.T) {
		value := []byte("test value")

		encoded, err := codec.EncodeValue(value)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(encoded, value) {
			t.Errorf("encode: values should be unchanged")
		}

		decoded, err := codec.DecodeValue(value)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(decoded, value) {
			t.Errorf("decode: values should be unchanged")
		}
	})
}

func TestEncodeFilter(t *testing.T) {
	t.Run("NoOpCodec", func(t *testing.T) {
		codec := NoOpCodec()

		tests := []struct {
			name     string
			pattern  string
			expected string
		}{
			{"specific key", "user.123", "user.123"},
			{"single wildcard", "user.*", "user.*"},
			{"multi wildcard", "user.>", "user.>"},
			{"complex pattern", "app.*.config.>", "app.*.config.>"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := codec.EncodeFilter(tt.pattern)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("expected %s, got %s", tt.expected, result)
				}
			})
		}
	})

	t.Run("Base64Codec", func(t *testing.T) {
		codec := Base64Codec()

		tests := []struct {
			name     string
			pattern  string
			expected string
		}{
			{"specific key", "user.123", "dXNlcg.MTIz"},
			{"single wildcard", "user.*", "dXNlcg.*"},
			{"multi wildcard", "user.>", "dXNlcg.>"},
			{"complex pattern", "app.*.config.>", "YXBw.*.Y29uZmln.>"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := codec.EncodeFilter(tt.pattern)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("expected %s, got %s", tt.expected, result)
				}
			})
		}
	})

	t.Run("PathCodec", func(t *testing.T) {
		codec := PathCodec()

		tests := []struct {
			name     string
			pattern  string
			expected string
		}{
			{"specific key", "/user/123", "_root_.user.123"},
			{"single wildcard", "/user/*", "_root_.user.*"},
			{"multi wildcard", "/user/>", "_root_.user.>"},
			{"complex pattern", "/app/*/config/>", "_root_.app.*.config.>"},
			{"no leading slash", "user/*", "user.*"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := codec.EncodeFilter(tt.pattern)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("expected %s, got %s", tt.expected, result)
				}
			})
		}
	})
}
