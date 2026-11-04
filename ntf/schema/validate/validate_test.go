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

package validate

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/nats-io/nats.go/micro"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	thingType = "io.example.v1.thing"
	otherType = "io.example.v1.other"

	// thingSchema requires name, refuses unknown fields, and holds a nested
	// object and an array, to give failures below the root.
	thingSchema = `{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"$id": "https://example.com/schemas/thing.json",
		"type": "object",
		"additionalProperties": false,
		"required": ["name"],
		"properties": {
			"name": {"type": "string", "minLength": 1},
			"when": {"type": "string", "format": "date-time"},
			"inner": {
				"type": "object",
				"properties": {"size": {"type": "integer", "minimum": 1}}
			},
			"items": {
				"type": "array",
				"items": {"type": "object", "properties": {"id": {"type": "string"}}}
			}
		}
	}`

	// otherSchema requires nothing.
	otherSchema = `{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"$id": "https://example.com/schemas/other.json",
		"type": "object",
		"additionalProperties": false,
		"properties": {"size": {"type": "integer"}}
	}`
)

// loadTest returns the schemas of the test types.
func loadTest(typ string) ([]byte, error) {
	switch typ {
	case thingType:
		return []byte(thingSchema), nil
	case otherType:
		return []byte(otherSchema), nil
	}

	return nil, fmt.Errorf("no schema of %s", typ)
}

// compileTest compiles the schema of typ.
func compileTest(t *testing.T, typ string) *jsonschema.Schema {
	t.Helper()

	schemas, err := Compile(map[string]string{"test": typ}, loadTest)
	if err != nil {
		t.Fatal(err)
	}

	return schemas["test"]
}

// fakeRequest is a micro.Request that records its answer.
type fakeRequest struct {
	data        []byte
	code        string
	description string
	responded   []byte
}

var _ micro.Request = (*fakeRequest)(nil)

func (r *fakeRequest) Respond(data []byte, _ ...micro.RespondOpt) error {
	r.responded = data
	return nil
}

func (r *fakeRequest) RespondJSON(v any, _ ...micro.RespondOpt) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	r.responded = data
	return nil
}

func (r *fakeRequest) Error(code, description string, _ []byte, _ ...micro.RespondOpt) error {
	r.code = code
	r.description = description
	return nil
}

func (r *fakeRequest) Data() []byte           { return r.data }
func (r *fakeRequest) Headers() micro.Headers { return nil }
func (r *fakeRequest) Subject() string        { return "test" }
func (r *fakeRequest) Reply() string          { return "reply" }

// TestCompileOncePerType proves endpoints that share a request type compile:
// the type is loaded once and they share the schema, while an endpoint of
// another type gets its own.
func TestCompileOncePerType(t *testing.T) {
	loads := map[string]int{}
	load := func(typ string) ([]byte, error) {
		loads[typ]++
		return loadTest(typ)
	}

	schemas, err := Compile(map[string]string{
		"stop":   thingType,
		"pause":  thingType,
		"resume": thingType,
		"list":   otherType,
	}, load)
	if err != nil {
		t.Fatal(err)
	}

	if len(schemas) != 4 {
		t.Fatalf("%d schemas, want 4", len(schemas))
	}
	if loads[thingType] != 1 || loads[otherType] != 1 {
		t.Errorf("loads = %v, want each type once", loads)
	}
	if schemas["stop"] != schemas["pause"] || schemas["stop"] != schemas["resume"] {
		t.Error("endpoints of one type do not share its schema")
	}
	if schemas["stop"] == schemas["list"] {
		t.Error("endpoints of two types share a schema")
	}

	if Description(schemas["stop"], []byte(`{"name":"x"}`)) != "" {
		t.Error("the schema of the shared type refused a valid body")
	}
	if Description(schemas["list"], []byte(`{}`)) != "" {
		t.Error("the schema of the other type refused a valid body")
	}
}

// TestCompileFailures proves what stops a compile is reported with the type it
// stopped at, and the error of load is kept.
func TestCompileFailures(t *testing.T) {
	errLoad := errors.New("load failed")

	t.Run("load", func(t *testing.T) {
		_, err := Compile(map[string]string{"test": thingType}, func(string) ([]byte, error) {
			return nil, errLoad
		})
		if !errors.Is(err, errLoad) {
			t.Errorf("err = %v, want %v", err, errLoad)
		}
	})

	cases := []struct {
		name   string
		schema string
		want   string
	}{
		{"not JSON", `{`, "schema of " + thingType + " is not JSON"},
		{"not a schema", `{"type":1}`, "could not compile schema of " + thingType},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(map[string]string{"test": thingType}, func(string) ([]byte, error) {
				return []byte(tc.schema), nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestCompileAssertsFormat proves format is enforced, which santhosh does not
// do unless told to.
func TestCompileAssertsFormat(t *testing.T) {
	sch := compileTest(t, thingType)

	want := "Invalid request: when: 'nope' is not valid date-time: "
	got := Description(sch, []byte(`{"name":"x","when":"nope"}`))
	if !strings.HasPrefix(got, want) {
		t.Errorf("description = %q, want the prefix %q", got, want)
	}
}

// TestHandlerRefuses proves a body the schema refuses is answered 001 with its
// description and never reaches the handler.
func TestHandlerRefuses(t *testing.T) {
	sch := compileTest(t, thingType)

	cases := []struct {
		name string
		body string
		want string
	}{
		{"unknown field", `{"name":"x","bogus":1}`, "Invalid request: additional properties 'bogus' not allowed"},
		{"wrong type", `{"name":1}`, "Invalid request: name: got number, want string"},
		{"null", `null`, "Invalid request: got null, want object"},
		{"empty", ``, "Invalid request: missing property 'name'"},
		{"whitespace", " \t\r\n", "Invalid request: missing property 'name'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ran := false
			req := &fakeRequest{data: []byte(tc.body)}
			Handler(sch, func(micro.Request) { ran = true })(req)

			if ran {
				t.Error("handler ran")
			}
			if req.code != "001" {
				t.Errorf("code = %q, want 001", req.code)
			}
			if req.description != tc.want {
				t.Errorf("description = %q, want %q", req.description, tc.want)
			}
		})
	}

	// A form feed is whitespace to Go but not to JSON, so the body is not blank.
	t.Run("not JSON whitespace", func(t *testing.T) {
		ran := false
		req := &fakeRequest{data: []byte("\f")}
		Handler(compileTest(t, otherType), func(micro.Request) { ran = true })(req)

		if ran {
			t.Error("handler ran")
		}
		if req.code != "001" || !strings.HasPrefix(req.description, "Invalid request: ") {
			t.Errorf("answer = %q %q, want 001", req.code, req.description)
		}
	})
}

// TestHandlerNilSchema proves an endpoint with no schema is refused when its
// handler is made, not when its first request arrives.
func TestHandlerNilSchema(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Handler accepted a nil schema")
		}
	}()

	Handler(nil, func(micro.Request) {})
}

// TestHandlerEmptyBody proves an empty or whitespace body reaches the handler
// as {}, on a request that still answers as the one it wraps.
func TestHandlerEmptyBody(t *testing.T) {
	sch := compileTest(t, otherType)

	for _, body := range [][]byte{nil, {}, []byte(" "), []byte("\n"), []byte(" \t\r\n ")} {
		t.Run(fmt.Sprintf("%q", body), func(t *testing.T) {
			var got []byte
			req := &fakeRequest{data: body}
			Handler(sch, func(req micro.Request) {
				got = req.Data()

				var into struct {
					Size int `json:"size"`
				}
				err := json.Unmarshal(req.Data(), &into)
				if err != nil {
					t.Errorf("unmarshal: %v", err)
				}

				if req.Subject() != "test" || req.Reply() != "reply" {
					t.Errorf("subject and reply = %q %q, want the wrapped request's", req.Subject(), req.Reply())
				}
				err = req.Respond([]byte("ok"))
				if err != nil {
					t.Error(err)
				}
			})(req)

			if string(got) != "{}" {
				t.Errorf("handler read %q, want {}", got)
			}
			if string(req.responded) != "ok" {
				t.Errorf("wrapped request answered %q, want ok", req.responded)
			}
		})
	}
}

// TestHandlerPassesBody proves a valid body reaches the handler as sent, the
// whitespace around it included, on the request it came in.
func TestHandlerPassesBody(t *testing.T) {
	sch := compileTest(t, otherType)

	for _, body := range []string{`{}`, `{"size":1}`, " {\"size\":1}\n"} {
		var got micro.Request
		req := &fakeRequest{data: []byte(body)}
		Handler(sch, func(req micro.Request) { got = req })(req)

		if got != micro.Request(req) {
			t.Errorf("body %q reached the handler on another request", body)
		}
		if req.code != "" {
			t.Errorf("body %q refused: %s", body, req.description)
		}
	}
}

// TestDescription proves the format of a refusal: the dotted path of the first
// failure by path, with no path for the request itself.
func TestDescription(t *testing.T) {
	sch := compileTest(t, thingType)

	cases := []struct {
		name string
		body string
		want string
	}{
		{"valid", `{"name":"x"}`, ""},
		{"request itself", `[]`, "Invalid request: got array, want object"},
		{"missing property", `{}`, "Invalid request: missing property 'name'"},
		{"empty reads as {}", ``, "Invalid request: missing property 'name'"},
		{"whitespace reads as {}", " \n", "Invalid request: missing property 'name'"},
		{"field", `{"name":""}`, "Invalid request: name: minLength: got 0, want 1"},
		{"nested field", `{"name":"x","inner":{"size":0}}`, "Invalid request: inner.size: minimum: got 0, want 1"},
		{"array element", `{"name":"x","items":[{"id":"a"},{"id":1}]}`, "Invalid request: items.1.id: got number, want string"},
		{"first by path", `{"name":"","inner":{"size":0}}`, "Invalid request: inner.size: minimum: got 0, want 1"},
		{"root before fields", `{"name":"","bogus":1}`, "Invalid request: additional properties 'bogus' not allowed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for range 20 {
				got := Description(sch, []byte(tc.body))
				if got != tc.want {
					t.Fatalf("description = %q, want %q", got, tc.want)
				}
			}
		})
	}

	t.Run("not JSON", func(t *testing.T) {
		got := Description(sch, []byte(`{`))
		if !strings.HasPrefix(got, "Invalid request: ") || len(got) == len("Invalid request: ") {
			t.Errorf("description = %q, want the reading error after the prefix", got)
		}
	})

	t.Run("capped", func(t *testing.T) {
		got := Description(sch, []byte(`{"name":"x","`+strings.Repeat("x", 4*MaxDescriptionLen)+`":1}`))
		if len(got) != MaxDescriptionLen || !strings.HasSuffix(got, "...") {
			t.Errorf("description of %d bytes ending %q, want %d ending ...", len(got), got[max(0, len(got)-10):], MaxDescriptionLen)
		}
	})

	t.Run("capped with a path", func(t *testing.T) {
		got := Description(sch, []byte(`{"name":"x","when":"`+strings.Repeat("x", 4*MaxDescriptionLen)+`"}`))
		if !strings.HasPrefix(got, "Invalid request: when: ") {
			t.Errorf("description = %q, want the path of when", got[:min(len(got), 40)])
		}
		if len(got) != MaxDescriptionLen || !strings.HasSuffix(got, "...") {
			t.Errorf("description of %d bytes ending %q, want %d ending ...", len(got), got[max(0, len(got)-10):], MaxDescriptionLen)
		}
	})
}

// TestCapDescription proves a description fits the cap, is left alone when it
// already does, and is not left holding part of a rune.
func TestCapDescription(t *testing.T) {
	if MaxDescriptionLen != 1024 {
		t.Errorf("MaxDescriptionLen = %d, want 1024", MaxDescriptionLen)
	}

	fits := strings.Repeat("x", MaxDescriptionLen)
	if CapDescription(fits) != fits {
		t.Error("a description at the cap was cut")
	}

	over := fits + "x"
	want := strings.Repeat("x", MaxDescriptionLen-3) + "..."
	got := CapDescription(over)
	if got != want {
		t.Errorf("cut of %d bytes = %d bytes, want %d", len(over), len(got), len(want))
	}

	// The cut lands inside the three bytes of the last rune.
	split := strings.Repeat("x", MaxDescriptionLen-4) + "€" + strings.Repeat("x", 10)
	got = CapDescription(split)
	if !utf8.ValidString(got) {
		t.Errorf("cut left an invalid rune: %q", got[len(got)-10:])
	}
	want = strings.Repeat("x", MaxDescriptionLen-4) + "..."
	if got != want {
		t.Errorf("cut ends %q, want %q", got[len(got)-10:], want[len(want)-10:])
	}
}
