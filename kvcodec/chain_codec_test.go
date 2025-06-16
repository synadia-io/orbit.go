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
	"errors"
	"strings"
	"testing"
)

func TestNewKeyChainCodec(t *testing.T) {
	_, err := NewKeyChainCodec()
	if err == nil {
		t.Fatal("expected error when creating chain with no codecs")
	}
	if !errors.Is(err, ErrNoCodecs) {
		t.Errorf("expected ErrNoCodecs, got: %v", err)
	}

	base64Codec := Base64Codec()
	pathCodec := PathCodec()

	chain, err := NewKeyChainCodec(base64Codec, pathCodec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chain == nil {
		t.Fatal("key chain codec should not be nil")
	}

}

func TestNewValueChainCodec(t *testing.T) {
	_, err := NewValueChainCodec()
	if err == nil {
		t.Fatal("expected error when creating chain with no codecs")
	}
	if !errors.Is(err, ErrNoCodecs) {
		t.Errorf("expected ErrNoCodecs, got: %v", err)
	}

	base64Codec := Base64Codec()
	noOp := NoOpCodec()

	chain, err := NewValueChainCodec(base64Codec, noOp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chain == nil {
		t.Fatal("value chain codec should not be nil")
	}

}

func TestKeyChainCodecBasicOperations(t *testing.T) {
	pathCodec := PathCodec()
	base64Codec := Base64Codec()

	chain, err := NewKeyChainCodec(pathCodec, base64Codec)
	if err != nil {
		t.Fatalf("failed to create chain codec: %v", err)
	}

	tests := []struct {
		input    string
		expected string
	}{
		{"/simple/key", "simple/key"},
		{"/foo/bar/baz", "foo/bar/baz"},
		{"/single", "single"},
		{"/with/*/wildcard", "with/*/wildcard"},
		{"/with/>", "with/>"},
		{"no/leading/slash", "no/leading/slash"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			encoded, err := chain.EncodeKey(tt.input)
			if err != nil {
				t.Fatalf("encode failed: %v", err)
			}

			// Should be different from original (path converted + base64 encoded)
			if encoded == tt.input {
				t.Errorf("encoded key should be different from original")
			}

			decoded, err := chain.DecodeKey(encoded)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			if decoded != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, decoded)
			}
		})
	}

}

func TestKeyChainCodecOrder(t *testing.T) {
	// Test that order matters: first codec applied first for encoding
	codec1 := &testCodec{prefix: "A"}
	codec2 := &testCodec{prefix: "B"}

	chain, err := NewKeyChainCodec(codec1, codec2)
	if err != nil {
		t.Fatalf("failed to create chain: %v", err)
	}

	// For key "test":
	// 1. codec1 encodes: "test" -> "A:test"
	// 2. codec2 encodes: "A:test" -> "B:A:test"
	encoded, err := chain.EncodeKey("test")
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	expected := "B:A:test"
	if encoded != expected {
		t.Errorf("expected %s, got %s", expected, encoded)
	}

	// Decoding should reverse the order:
	// 1. codec2 decodes: "B:A:test" -> "A:test"
	// 2. codec1 decodes: "A:test" -> "test"
	decoded, err := chain.DecodeKey(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded != "test" {
		t.Errorf("expected test, got %s", decoded)
	}
}

func TestChainCodecFilterableSupport(t *testing.T) {
	t.Run("AllFilterableCodecs", func(t *testing.T) {
		// Test with codecs that ALL support filtering
		base64Codec := Base64Codec()
		noOpCodec := NoOpCodec()
		pathCodec := PathCodec()

		chain, err := NewKeyChainCodec(base64Codec, noOpCodec, pathCodec)
		if err != nil {
			t.Fatalf("failed to create chain codec: %v", err)
		}

		// Should implement FilterableKeyCodec since all codecs support it
		filterable, ok := interface{}(chain).(FilterableKeyCodec)
		if !ok {
			t.Fatal("chain should implement FilterableKeyCodec when all codecs support filtering")
		}

		pattern := "orders.*.status"
		encoded, err := filterable.EncodeFilter(pattern)
		if err != nil {
			t.Fatalf("filter encode failed: %v", err)
		}

		// Should preserve wildcards in the result
		if !strings.Contains(encoded, "*") {
			t.Errorf("encoded filter should preserve wildcards, got: %s", encoded)
		}
	})

	t.Run("NonFilterableCodecInChain", func(t *testing.T) {
		// Test with one non-filterable codec in the chain
		base64Codec := Base64Codec()
		testCodec := &testCodec{prefix: "test"} // testCodec doesn't implement FilterableKeyCodec

		chain, err := NewKeyChainCodec(base64Codec, testCodec)
		if err != nil {
			t.Fatalf("failed to create chain codec: %v", err)
		}

		// Should implement FilterableKeyCodec at type level (since it has the method)
		filterable, ok := interface{}(chain).(FilterableKeyCodec)
		if !ok {
			t.Fatal("chain should implement FilterableKeyCodec interface")
		}

		// But calling EncodeFilter should return an error since testCodec doesn't support filtering
		pattern := "orders.*.status"
		_, err = filterable.EncodeFilter(pattern)
		if err == nil {
			t.Fatal("EncodeFilter should fail when chain contains non-filterable codec")
		}

		// Error should indicate wildcard not supported
		if !strings.Contains(err.Error(), "does not implement FilterableKeyCodec") {
			t.Errorf("expected error about FilterableKeyCodec, got: %v", err)
		}
	})
}

func TestChainCodecErrorHandling(t *testing.T) {
	errorCodec := &errorCodec{}
	base64Codec := Base64Codec()

	chain, err := NewKeyChainCodec(errorCodec, base64Codec)
	if err != nil {
		t.Fatalf("failed to create chain codec: %v", err)
	}

	t.Run("EncodeError", func(t *testing.T) {
		_, err := chain.EncodeKey("test")
		if err == nil {
			t.Fatal("expected error from error codec")
		}
		if !errors.Is(err, ErrKeyEncodeFailed) {
			t.Errorf("expected ErrKeyEncodeFailed, got: %v", err)
		}
	})

	t.Run("DecodeError", func(t *testing.T) {
		_, err := chain.DecodeKey("test")
		if err == nil {
			t.Fatal("expected error from error codec")
		}
		if !errors.Is(err, ErrKeyDecodeFailed) {
			t.Errorf("expected ErrKeyDecodeFailed, got: %v", err)
		}
	})
}

// Test helpers

type testCodec struct {
	prefix string
}

func (t *testCodec) EncodeKey(key string) (string, error) {
	return t.prefix + ":" + key, nil
}

func (t *testCodec) DecodeKey(key string) (string, error) {
	if !strings.HasPrefix(key, t.prefix+":") {
		return "", nil // Invalid format, but don't error for simplicity
	}
	return key[len(t.prefix)+1:], nil
}

func (t *testCodec) EncodeValue(value []byte) ([]byte, error) {
	return append([]byte(t.prefix+":"), value...), nil
}

func (t *testCodec) DecodeValue(value []byte) ([]byte, error) {
	prefix := []byte(t.prefix + ":")
	if !bytes.HasPrefix(value, prefix) {
		return value, nil // Invalid format, but don't error for simplicity
	}
	return value[len(prefix):], nil
}

type errorCodec struct{}

func (e *errorCodec) EncodeKey(key string) (string, error) {
	return "", errors.New("encode key error")
}

func (e *errorCodec) DecodeKey(key string) (string, error) {
	return "", errors.New("decode key error")
}

func (e *errorCodec) EncodeValue(value []byte) ([]byte, error) {
	return nil, errors.New("encode value error")
}

func (e *errorCodec) DecodeValue(value []byte) ([]byte, error) {
	return nil, errors.New("decode value error")
}
