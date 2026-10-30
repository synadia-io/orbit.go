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

package ntf

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/nats-io/nats.go/micro"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// maxInvalidRequestLen caps the description of a request refused as invalid.
// The description travels in a header of the reply, and a santhosh message
// quotes what the request sent, such as the names of unknown fields, so an
// uncapped one could push the reply past the server's max payload and the
// reply would fail.
const maxInvalidRequestLen = 1024

// emptyBodyEndpoints holds the endpoints whose empty request body is
// validated as {}.
var emptyBodyEndpoints = map[string]bool{"status": true, "reset": true, "list": true, "clock": true, "schema": true}

// compileRequestSchemas compiles the request schema of every endpoint in
// api.Endpoints, by endpoint name. Each embedded schema is added to the
// compiler as a resource, so nothing is fetched.
func compileRequestSchemas() (map[string]*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.AssertFormat()

	schemas := map[string]*jsonschema.Schema{}
	for _, ep := range api.Endpoints {
		schema, err := api.Schema(ep.RequestType)
		if err != nil {
			return nil, err
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
		if err != nil {
			return nil, fmt.Errorf("schema of %s is not JSON: %w", ep.RequestType, err)
		}

		url := ep.RequestType + ".json"
		err = c.AddResource(url, doc)
		if err != nil {
			return nil, fmt.Errorf("could not add schema of %s: %w", ep.RequestType, err)
		}
		schemas[ep.Name], err = c.Compile(url)
		if err != nil {
			return nil, fmt.Errorf("could not compile schema of %s: %w", ep.RequestType, err)
		}
	}

	return schemas, nil
}

// validated returns a handler that validates the body of each request against
// sch before h runs, and answers 001 itself when the body does not match. When
// emptyAsObject is set an empty body is validated as {}; h still reads the
// body as sent. It wraps the handler an endpoint registers, perRequest
// included, so the body is validated before a request is detached.
func validated(sch *jsonschema.Schema, emptyAsObject bool, h micro.HandlerFunc) micro.HandlerFunc {
	return func(req micro.Request) {
		data := req.Data()
		if emptyAsObject && len(data) == 0 {
			data = []byte("{}")
		}

		desc := invalidRequest(sch, data)
		if desc != "" {
			req.Error("001", desc, nil)
			return
		}

		h(req)
	}
}

// validationFailure is one leaf failure of a validation: the dotted path of
// the value that failed in the request, and santhosh's message.
type validationFailure struct {
	path    string
	message string
}

// invalidRequest validates data against sch and returns the description a
// refusal answers, or empty when data is valid. The leaf failures are sorted
// by path and the first is reported, as Invalid request: <path>: <message>,
// or Invalid request: <message> for the request itself, so a request always
// gets the same answer. A body that is not JSON is reported with the error
// that stopped reading it.
func invalidRequest(sch *jsonschema.Schema, data []byte) string {
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return capDescription("Invalid request: " + err.Error())
	}

	err = sch.Validate(inst)
	if err == nil {
		return ""
	}

	var verr *jsonschema.ValidationError
	if !errors.As(err, &verr) {
		return capDescription("Invalid request: " + err.Error())
	}

	failures := validationFailures(verr, nil)
	slices.SortFunc(failures, func(a, b validationFailure) int {
		return cmp.Or(cmp.Compare(a.path, b.path), cmp.Compare(a.message, b.message))
	})

	first := failures[0]
	if first.path == "" {
		return capDescription("Invalid request: " + first.message)
	}

	return capDescription(fmt.Sprintf("Invalid request: %s: %s", first.path, first.message))
}

// validationFailures appends the leaf failures of verr, those with no cause
// below them, to failures.
func validationFailures(verr *jsonschema.ValidationError, failures []validationFailure) []validationFailure {
	if len(verr.Causes) == 0 {
		return append(failures, validationFailure{
			path:    strings.Join(verr.InstanceLocation, "."),
			message: verr.BasicOutput().Error.String(),
		})
	}

	for _, cause := range verr.Causes {
		failures = validationFailures(cause, failures)
	}

	return failures
}

// capDescription cuts desc to maxInvalidRequestLen bytes, ending a cut one
// with "...", and drops a rune the cut split.
func capDescription(desc string) string {
	if len(desc) <= maxInvalidRequestLen {
		return desc
	}

	return strings.ToValidUTF8(desc[:maxInvalidRequestLen-3], "") + "..."
}

// endpointSchemas returns the request and response schemas of every endpoint
// in api.Endpoints, sorted by name.
func endpointSchemas() ([]api.EndpointSchema, error) {
	endpoints := make([]api.EndpointSchema, 0, len(api.Endpoints))
	for _, ep := range api.Endpoints {
		request, err := api.Schema(ep.RequestType)
		if err != nil {
			return nil, err
		}
		response, err := api.Schema(ep.ResponseType)
		if err != nil {
			return nil, err
		}

		endpoints = append(endpoints, api.EndpointSchema{
			Name:        ep.Name,
			Description: ep.Description,
			Request:     json.RawMessage(request),
			Response:    json.RawMessage(response),
		})
	}

	slices.SortFunc(endpoints, func(a, b api.EndpointSchema) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return endpoints, nil
}

// schemaHandler returns the handler of the schema endpoint, which answers
// endpoints.
func schemaHandler(endpoints []api.EndpointSchema) micro.HandlerFunc {
	return func(req micro.Request) {
		req.RespondJSON(api.SchemaResponse{Type: api.TypeSchemaResponse, Endpoints: endpoints})
	}
}
