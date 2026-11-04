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

// Package validate validates the requests of an NTF service against their
// committed JSON Schemas, by the rules every service follows: a request is
// validated before its handler runs, one its schema refuses answers 001 with
// Invalid request: <dotted.path>: <message>, the description of a refusal is
// capped, and an empty or whitespace body reads as {}.
//
// It knows nothing of any one service. The caller gives the request type of
// each endpoint and how to load a schema by its type, and keeps its schemas and
// its endpoints to itself.
//
// It imports santhosh-tekuri/jsonschema and micro, so a package that clients
// import must not import this one.
package validate

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go/micro"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

// MaxDescriptionLen caps the description of a refusal. The description travels
// in a header of the reply, and it can quote what the request sent, such as the
// names of unknown fields, so an uncapped one could push the reply past the
// server's max payload and the reply would fail.
const MaxDescriptionLen = 1024

// Compile compiles the request schema of every endpoint in types, which holds
// the request type of each endpoint by endpoint name, and returns them by
// endpoint name. load gives the schema of a type. Each schema is added to the
// compiler as a resource, so nothing is fetched.
//
// A type is loaded and compiled once and the endpoints that share it share the
// result, since a compiler refuses a second resource of one URL.
func Compile(types map[string]string, load func(typ string) ([]byte, error)) (map[string]*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.AssertFormat()

	byType := map[string]*jsonschema.Schema{}
	schemas := make(map[string]*jsonschema.Schema, len(types))
	for _, name := range slices.Sorted(maps.Keys(types)) {
		typ := types[name]
		if byType[typ] == nil {
			schema, err := load(typ)
			if err != nil {
				return nil, err
			}
			doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
			if err != nil {
				return nil, fmt.Errorf("schema of %s is not JSON: %w", typ, err)
			}

			url := typ + ".json"
			err = c.AddResource(url, doc)
			if err != nil {
				return nil, fmt.Errorf("could not add schema of %s: %w", typ, err)
			}
			byType[typ], err = c.Compile(url)
			if err != nil {
				return nil, fmt.Errorf("could not compile schema of %s: %w", typ, err)
			}
		}
		schemas[name] = byType[typ]
	}

	return schemas, nil
}

// Handler returns a handler that validates the body of each request against
// sch before h runs, and answers 001 itself when the body does not match. An
// empty or whitespace body reaches h as {}, so a handler can unmarshal
// req.Data() whatever was sent. It wraps the handler an endpoint registers, so
// a body is validated before anything that handler does with the request.
//
// A nil sch panics here rather than on the first request, so an endpoint left
// out of Compile stops the service as it starts.
func Handler(sch *jsonschema.Schema, h micro.HandlerFunc) micro.HandlerFunc {
	if sch == nil {
		panic("validate: Handler needs a schema")
	}

	return func(req micro.Request) {
		desc := Description(sch, req.Data())
		if desc != "" {
			req.Error("001", desc, nil)
			return
		}

		if blank(req.Data()) {
			req = emptyRequest{req}
		}

		h(req)
	}
}

// emptyRequest is a request that reads as {}, answered as the one it wraps.
type emptyRequest struct {
	micro.Request
}

// Data returns {}.
func (emptyRequest) Data() []byte { return []byte("{}") }

// blank reports whether data holds nothing but JSON whitespace.
func blank(data []byte) bool {
	return len(bytes.Trim(data, " \t\r\n")) == 0
}

// validationFailure is one leaf failure of a validation: the dotted path of
// the value that failed in the request, and santhosh's message.
type validationFailure struct {
	path    string
	message string
}

// Description validates data against sch and returns the description a refusal
// answers, capped by CapDescription, or empty when data is valid. Empty or
// whitespace data reads as {}. The leaf failures are sorted by path and the
// first is reported, as Invalid request: <path>: <message>, or Invalid request:
// <message> for the request itself, so a request always gets the same failure
// reported. Only santhosh's list of several unknown fields of one object can
// differ between answers, since it is built in map order. Data that is not JSON
// is reported with the error that stopped reading it.
func Description(sch *jsonschema.Schema, data []byte) string {
	if blank(data) {
		data = []byte("{}")
	}

	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return CapDescription("Invalid request: " + err.Error())
	}

	err = sch.Validate(inst)
	if err == nil {
		return ""
	}

	var verr *jsonschema.ValidationError
	if !errors.As(err, &verr) {
		return CapDescription("Invalid request: " + err.Error())
	}

	failures := validationFailures(verr, inst, nil)
	slices.SortFunc(failures, func(a, b validationFailure) int {
		return cmp.Or(cmp.Compare(a.path, b.path), cmp.Compare(a.message, b.message))
	})

	first := failures[0]
	if first.path == "" {
		return CapDescription("Invalid request: " + first.message)
	}

	return CapDescription(fmt.Sprintf("Invalid request: %s: %s", first.path, first.message))
}

// validationFailures appends the leaf failures of verr, those with no cause
// below them, to failures. A refused key of an object counts as a leaf: its
// cause is the check of the key on its own, which has no location in inst and
// says nothing of which key it was.
func validationFailures(verr *jsonschema.ValidationError, inst any, failures []validationFailure) []validationFailure {
	if refused, ok := verr.ErrorKind.(*kind.PropertyNames); ok {
		// santhosh gives this failure a location that shares memory with the
		// walk, so the walk overwrites it. Only its depth can be trusted.
		location, _ := keyHolder(inst, refused.Property, len(verr.InstanceLocation))

		leaf := *verr
		leaf.Causes = nil

		return append(failures, validationFailure{
			path:    strings.Join(location, "."),
			message: leaf.BasicOutput().Error.String(),
		})
	}

	if len(verr.Causes) == 0 {
		return append(failures, validationFailure{
			path:    strings.Join(verr.InstanceLocation, "."),
			message: verr.BasicOutput().Error.String(),
		})
	}

	for _, cause := range verr.Causes {
		failures = validationFailures(cause, inst, failures)
	}

	return failures
}

// keyHolder returns the location of the first object depth levels into v that
// holds key, walking keys in sorted order so the answer does not vary.
//
// Depth and key are all a refused key leaves to go on, so the match can be the
// wrong object: one at the same depth that holds the same key, sorts first and
// has no rule refusing it is named in place of the object that was refused. The
// request is still refused, and for the right key.
func keyHolder(v any, key string, depth int) ([]string, bool) {
	switch v := v.(type) {
	case map[string]any:
		if depth == 0 {
			_, ok := v[key]
			return nil, ok
		}

		for _, name := range slices.Sorted(maps.Keys(v)) {
			location, ok := keyHolder(v[name], key, depth-1)
			if ok {
				return append([]string{name}, location...), true
			}
		}

	case []any:
		if depth == 0 {
			return nil, false
		}

		for i, item := range v {
			location, ok := keyHolder(item, key, depth-1)
			if ok {
				return append([]string{strconv.Itoa(i)}, location...), true
			}
		}
	}

	return nil, false
}

// CapDescription cuts desc to MaxDescriptionLen bytes, ending a cut one with
// "...", and drops a rune the cut split.
func CapDescription(desc string) string {
	if len(desc) <= MaxDescriptionLen {
		return desc
	}

	return strings.ToValidUTF8(desc[:MaxDescriptionLen-3], "") + "..."
}
