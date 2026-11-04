// Copyright 2026 Synadia Communications Inc.
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

package gen

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	invopop "github.com/invopop/jsonschema"
)

const (
	testType = "io.example.v1.thing"
	testURL  = "https://example.com/schemas/thing.json"
)

// opaque stands for a type of another package, which cannot carry a hook.
type opaque struct {
	Hidden string `json:"hidden"`
}

type thing struct {
	Type   string `json:"type,omitempty"`
	Opaque opaque `json:"opaque"`
}

// schemaHead is the part of a generated schema these tests read.
type schemaHead struct {
	ID         string `json:"$id"`
	Title      string `json:"title"`
	Properties struct {
		Type struct {
			Const string `json:"const"`
		} `json:"type"`
		Opaque struct {
			Type       string          `json:"type"`
			Order      json.RawMessage `json:"x-order"`
			Properties json.RawMessage `json:"properties"`
		} `json:"opaque"`
	} `json:"properties"`
}

func parseHead(t *testing.T, data []byte) schemaHead {
	t.Helper()

	var head schemaHead
	err := json.Unmarshal(data, &head)
	if err != nil {
		t.Fatal(err)
	}

	return head
}

// TestCallerIdentity proves the $id, the title and the type const are the
// caller's, which the tester schemas alone would not show of a base URL or
// prefix left in this package.
func TestCallerIdentity(t *testing.T) {
	cases := []struct {
		name    string
		reflect func(reflect.Type, string, string, Mapper) ([]byte, error)
	}{
		{"request", Request},
		{"response", Response},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.reflect(reflect.TypeFor[thing](), testType, testURL, nil)
			if err != nil {
				t.Fatal(err)
			}
			head := parseHead(t, data)

			if head.ID != testURL {
				t.Errorf("$id is %q, want %q", head.ID, testURL)
			}
			if head.Title != testType {
				t.Errorf("title is %q, want %q", head.Title, testType)
			}
			if head.Properties.Type.Const != testType {
				t.Errorf("type const is %q, want %q", head.Properties.Type.Const, testType)
			}
		})
	}
}

// TestMapper proves a Mapper replaces the type it maps, keeping the key order
// of a raw object in Extras, and leaves the other types to the reflector. The
// tester schemas use no Mapper, so they do not show this.
func TestMapper(t *testing.T) {
	mapper := func(rt reflect.Type) *invopop.Schema {
		if rt != reflect.TypeFor[opaque]() {
			return nil
		}

		return &invopop.Schema{Type: "object", Extras: map[string]any{"x-order": json.RawMessage(`{"z":1,"a":2}`)}}
	}

	data, err := Request(reflect.TypeFor[thing](), testType, testURL, mapper)
	if err != nil {
		t.Fatal(err)
	}
	head := parseHead(t, data)

	got := head.Properties.Opaque
	if got.Type != "object" {
		t.Errorf("opaque type is %q, want object", got.Type)
	}
	if got.Properties != nil {
		t.Errorf("opaque properties are %s, want none", got.Properties)
	}
	z := bytes.Index(got.Order, []byte(`"z"`))
	a := bytes.Index(got.Order, []byte(`"a"`))
	if z < 0 || a < 0 || z > a {
		t.Errorf("opaque x-order is %s, want key z before key a", got.Order)
	}
	if head.Properties.Type.Const != testType {
		t.Errorf("type const is %q, want %q", head.Properties.Type.Const, testType)
	}
}

// TestNestedTypeOfSameName proves a nested type does not replace a root of the
// same name, as a type of another package could.
func TestNestedTypeOfSameName(t *testing.T) {
	type nested = thing
	type thing struct {
		Type  string `json:"type,omitempty"`
		Inner nested `json:"inner"`
	}

	data, err := Response(reflect.TypeFor[thing](), testType, testURL, nil)
	if err != nil {
		t.Fatal(err)
	}

	var got struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	err = json.Unmarshal(data, &got)
	if err != nil {
		t.Fatal(err)
	}
	_, ok := got.Properties["inner"]
	if !ok {
		t.Errorf("root has no inner property: %s", data)
	}
}

// TestNoTypeProperty proves a type with nowhere to hold its type id is an
// error, whether it is a struct without the property or not a struct at all.
func TestNoTypeProperty(t *testing.T) {
	cases := []struct {
		name string
		typ  reflect.Type
	}{
		{"struct", reflect.TypeFor[opaque]()},
		{"string", reflect.TypeFor[string]()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Response(tc.typ, testType, testURL, nil)
			if !errors.Is(err, ErrNoTypeProperty) {
				t.Errorf("got %v, want ErrNoTypeProperty", err)
			}
		})
	}
}
