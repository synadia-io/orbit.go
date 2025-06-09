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
	"testing"
)

// User is a test struct for JSON codec
type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestNoOpCodec(t *testing.T) {
	codec := NoOpCodec

	t.Run("string encoding", func(t *testing.T) {
		str := "test"
		data, err := codec.Encode(str)
		if err != nil {
			t.Fatalf("NoOpCodec failed to encode string: %v", err)
		}
		if string(data) != str {
			t.Fatalf("Expected encoded data to be %q, got %q", str, data)
		}

		// Test decoding back to string
		var decodedStr string
		err = codec.Decode(data, &decodedStr)
		if err != nil {
			t.Fatalf("NoOpCodec failed to decode string: %v", err)
		}
		if decodedStr != str {
			t.Fatalf("Expected decoded string to be %q, got %q", str, decodedStr)
		}

		// check if codec is valid for string type
		if !codec.ValidType(str) {
			t.Fatal("NoOpCodec should be valid for string type")
		}
		if codec.ValidType(123) {
			t.Fatal("NoOpCodec should not be valid for int type")
		}
	})

	t.Run("bytes encoding", func(t *testing.T) {
		// Test encoding a byte slice
		byteData := []byte("byte data")
		data, err := codec.Encode(byteData)
		if err != nil {
			t.Fatalf("NoOpCodec failed to encode byte slice: %v", err)
		}
		if string(data) != string(byteData) {
			t.Fatalf("Expected encoded data to be %q, got %q", byteData, data)
		}
		// Test decoding back to byte slice
		var decodedBytes []byte
		err = codec.Decode(data, &decodedBytes)
		if err != nil {
			t.Fatalf("NoOpCodec failed to decode byte slice: %v", err)
		}
		if string(decodedBytes) != string(byteData) {
			t.Fatalf("Expected decoded byte slice to be %q, got %q", byteData, decodedBytes)
		}
	})
}

func TestJSONCodec(t *testing.T) {
	codec := JSONCodec
	user := User{Name: "Alice", Age: 30}
	data, err := codec.Encode(user)
	if err != nil {
		t.Fatalf("JSONCodec failed to encode: %v", err)
	}
	expected := `{"name":"Alice","age":30}`
	if string(data) != expected {
		t.Fatalf("Expected encoded data to be %q, got %q", expected, data)
	}
	data = []byte(`{"name":"Alice","age":30}`)
	var decodedUser User
	err = codec.Decode(data, &decodedUser)
	if err != nil {
		t.Fatalf("JSONCodec failed to decode: %v", err)
	}
	if decodedUser != user {
		t.Fatalf("Expected decoded user to be {Name: Alice, Age: 30}, got %+v", decodedUser)
	}
	if !codec.ValidType(User{}) {
		t.Fatal("JSONCodec should be valid for User type")
	}
	if codec.ValidType(func() {}) {
		t.Fatal("JSONCodec should not be valid for func type")
	}
}

func TestSlashCodec(t *testing.T) {
	codec := SlashCodec

	t.Run("slash encoding", func(t *testing.T) {
		str := "test/string"
		data, err := codec.Encode(str)
		if err != nil {
			t.Fatalf("SlashCodec failed to encode string: %v", err)
		}
		expected := "test.string"
		if string(data) != expected {
			t.Fatalf("Expected encoded data to be %q, got %q", expected, data)
		}

		var decodedStr string
		err = codec.Decode(data, &decodedStr)
		if err != nil {
			t.Fatalf("SlashCodec failed to decode string: %v", err)
		}
		if decodedStr != str {
			t.Fatalf("Expected decoded string to be %q, got %q", str, decodedStr)
		}

		if !codec.ValidType(str) {
			t.Fatal("SlashCodec should be valid for string type")
		}
		if codec.ValidType(123) {
			t.Fatal("SlashCodec should not be valid for int type")
		}
	})

	t.Run("slash bytes encoding", func(t *testing.T) {
		byteData := []byte("byte/test")
		data, err := codec.Encode(byteData)
		if err != nil {
			t.Fatalf("SlashCodec failed to encode byte slice: %v", err)
		}
		expected := []byte("byte.test")
		if string(data) != string(expected) {
			t.Fatalf("Expected encoded data to be %q, got %q", expected, data)
		}

		var decodedBytes []byte
		err = codec.Decode(data, &decodedBytes)
		if err != nil {
			t.Fatalf("SlashCodec failed to decode byte slice: %v", err)
		}
		if string(decodedBytes) != string(byteData) {
			t.Fatalf("Expected decoded byte slice to be %q, got %q", byteData, decodedBytes)
		}
	})
}
