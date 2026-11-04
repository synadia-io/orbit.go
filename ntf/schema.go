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
	"cmp"
	"encoding/json"
	"slices"

	"github.com/nats-io/nats.go/micro"

	"github.com/synadia-io/orbit.go/ntf/api"
)

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
