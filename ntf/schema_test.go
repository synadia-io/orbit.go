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
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// rawServiceError sends data as is to subject and returns the service error
// code and description of the answer, both empty when it succeeded.
func rawServiceError(t *testing.T, nc *nats.Conn, subject string, data []byte) (string, string) {
	t.Helper()

	msg, err := nc.Request(subject, data, 30*time.Second)
	if err != nil {
		t.Fatalf("request %s: %v", subject, err)
	}

	return msg.Header.Get("Nats-Service-Error-Code"), msg.Header.Get("Nats-Service-Error")
}

// validRequests holds a request body for every endpoint that its request
// schema accepts, by endpoint name, without a type.
var validRequests = map[string]string{
	"create.server":        `{}`,
	"create.cluster":       `{"servers":2}`,
	"create.super-cluster": `{"servers":2,"clusters":2}`,
	"stop.server":          `{"name":"x"}`,
	"stop.instance":        `{"instance_id":"x"}`,
	"start.server":         `{"name":"x"}`,
	"start.instance":       `{"instance_id":"x"}`,
	"update.server":        `{"name":"x"}`,
	"reload.server":        `{"name":"x"}`,
	"shape.set":            `{"instance_id":"x"}`,
	"shape.clear":          `{"instance_id":"x"}`,
	"shape.report":         `{"instance_id":"x"}`,
	"shape.list":           `{"instance_id":"x"}`,
	"toxic.add":            `{"instance_id":"x"}`,
	"toxic.remove":         `{"instance_id":"x"}`,
	"toxic.clear":          `{"instance_id":"x"}`,
	"toxic.report":         `{"instance_id":"x"}`,
	"status":               `{}`,
	"reset":                `{}`,
	"destroy":              `{"instance_id":"x"}`,
	"list":                 `{}`,
	"clock":                `{}`,
	"schema":               `{}`,
}

// TestRequestWrongTypeRefused proves every endpoint validates its request
// before its handler runs: a request with a type other than the endpoint's
// request id is refused with 001, naming the type and the id it must be.
func TestRequestWrongTypeRefused(t *testing.T) {
	svc := startTestService(t)

	if len(validRequests) != len(api.Endpoints) {
		t.Fatalf("validRequests holds %d endpoints, api.Endpoints %d", len(validRequests), len(api.Endpoints))
	}

	for _, ep := range api.Endpoints {
		t.Run(ep.Name, func(t *testing.T) {
			valid, ok := validRequests[ep.Name]
			if !ok {
				t.Fatalf("no valid request for %s", ep.Name)
			}
			body := `{"type":"wrong",` + strings.TrimPrefix(valid, "{")
			if valid == `{}` {
				body = `{"type":"wrong"}`
			}

			code, desc := rawServiceError(t, svc.nc, "tester."+ep.Name, []byte(body))
			if code != "001" {
				t.Errorf("code = %q (%s), want 001", code, desc)
			}
			want := fmt.Sprintf("Invalid request: type: value must be '%s'", ep.RequestType)
			if desc != want {
				t.Errorf("description = %q, want %q", desc, want)
			}
		})
	}
}

// TestRequestTypeOptional proves a request is accepted without a type and
// with its endpoint's request id as the type.
func TestRequestTypeOptional(t *testing.T) {
	svc := startTestService(t)

	var status api.StatusResponse
	mustRequest(t, svc.nc, "tester.status", api.StatusRequest{}, &status)
	mustRequest(t, svc.nc, "tester.status", api.StatusRequest{Type: api.TypeStatusRequest}, &status)

	var list api.ListResponse
	mustRequest(t, svc.nc, "tester.list", api.ListRequest{}, &list)
	mustRequest(t, svc.nc, "tester.list", api.ListRequest{Type: api.TypeListRequest}, &list)

	// A destroy of an unknown instance passed validation when its handler
	// answers 404.
	code, desc := serviceError(t, svc.nc, "tester.destroy", api.DestroyRequest{InstanceID: "nope"})
	if code != "404" {
		t.Errorf("destroy without a type: code = %q (%s), want 404", code, desc)
	}
	code, desc = serviceError(t, svc.nc, "tester.destroy", api.DestroyRequest{Type: api.TypeDestroyRequest, InstanceID: "nope"})
	if code != "404" {
		t.Errorf("destroy with its type: code = %q (%s), want 404", code, desc)
	}
}

// TestShapeSetNilRulesValid proves a shape.set request whose set has nil
// Rules, which marshal as null, passes validation and reaches the handler.
func TestShapeSetNilRulesValid(t *testing.T) {
	svc := startTestService(t)

	req := api.ShapeSetRequest{InstanceID: "nope", Set: api.ShapingSet{ID: "s", ConnectionName: "c"}}
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(payload, []byte(`"rules":null`)) {
		t.Fatalf("request %s does not send rules as null", payload)
	}

	code, desc := rawServiceError(t, svc.nc, "tester.shape.set", payload)
	if code == "001" {
		t.Errorf("shape.set with nil rules refused as invalid: %s", desc)
	}
}

// TestRequestValidationRefusals proves what a request its schema refuses is
// answered with: 001 and the first leaf failure by path, its santhosh message
// passed through, the same on every request, and a body that is not JSON
// refused alike.
func TestRequestValidationRefusals(t *testing.T) {
	svc := startTestService(t)

	cases := []struct {
		name    string
		subject string
		body    string
		want    string
	}{
		{"unknown field", "tester.status", `{"bogus":1}`, "Invalid request: additional properties 'bogus' not allowed"},
		{"field in another case", "tester.destroy", `{"Instance_ID":"x"}`, "Invalid request: additional properties 'Instance_ID' not allowed"},
		{"missing field", "tester.destroy", `{}`, "Invalid request: missing property 'instance_id'"},
		{"wrong field type", "tester.clock", `{"extra":"a"}`, "Invalid request: extra: got string, want array"},
		{"toxic attribute", "tester.toxic.add", `{"instance_id":"x","toxic":{"name":"x","type":"bandwidth","attributes":{"rate":0}}}`, "Invalid request: toxic.attributes.rate: minimum: got 0, want 1"},
		{"unknown toxic attribute", "tester.toxic.add", `{"instance_id":"x","toxic":{"name":"x","type":"latency","attributes":{"bogus":1}}}`, "Invalid request: toxic.attributes: additional properties 'bogus' not allowed"},
		{"too few servers", "tester.create.cluster", `{"servers":1}`, "Invalid request: servers: minimum: got 1, want 2"},
		{"empty server name", "tester.stop.server", `{"name":""}`, "Invalid request: name: minLength: got 0, want 1"},
		{"array element", "tester.shape.set", `{"instance_id":"x","set":{"id":"s","connection_name":"c","rules":[{"id":"r","match":{},"action":"drop","nth":"1"}]}}`, "Invalid request: set.rules.0.nth: got string, want integer"},
		{"not an object", "tester.status", `[]`, "Invalid request: got array, want object"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, desc := rawServiceError(t, svc.nc, tc.subject, []byte(tc.body))
			if code != "001" {
				t.Errorf("code = %q (%s), want 001", code, desc)
			}
			if desc != tc.want {
				t.Errorf("description = %q, want %q", desc, tc.want)
			}
		})
	}

	t.Run("not JSON", func(t *testing.T) {
		for _, subject := range []string{"tester.status", "tester.create.server", "tester.toxic.add"} {
			code, desc := rawServiceError(t, svc.nc, subject, []byte("{"))
			if code != "001" {
				t.Errorf("%s: code = %q (%s), want 001", subject, code, desc)
			}
			if !strings.HasPrefix(desc, "Invalid request: ") {
				t.Errorf("%s: description = %q, want an Invalid request: prefix", subject, desc)
			}
		}
	})

	t.Run("two bad fields answer alike", func(t *testing.T) {
		body := []byte(`{"servers":1,"clusters":1,"jetstream":"yes"}`)
		want := "Invalid request: clusters: minimum: got 1, want 2"
		for range 20 {
			code, desc := rawServiceError(t, svc.nc, "tester.create.super-cluster", body)
			if code != "001" || desc != want {
				t.Fatalf("answer = %q %q, want 001 %q", code, desc, want)
			}
		}
	})

	t.Run("description capped", func(t *testing.T) {
		body := []byte(`{"` + strings.Repeat("x", 4*maxInvalidRequestLen) + `":1}`)
		code, desc := rawServiceError(t, svc.nc, "tester.status", body)
		if code != "001" {
			t.Errorf("code = %q, want 001", code)
		}
		if len(desc) != maxInvalidRequestLen || !strings.HasSuffix(desc, "...") {
			t.Errorf("description of %d bytes ending %q, want %d ending ...", len(desc), desc[max(0, len(desc)-10):], maxInvalidRequestLen)
		}
	})
}

// TestRequestEmptyBody proves reset, list and schema, as status and clock,
// treat an empty body as {} and refuse a body their schema does not accept,
// while any other endpoint refuses an empty body.
func TestRequestEmptyBody(t *testing.T) {
	svc := startTestService(t)

	var reset api.ResetResponse
	mustRequest(t, svc.nc, "tester.reset", nil, &reset)
	if !reset.Shutdown {
		t.Errorf("reset with an empty body = %+v, want shutdown", reset)
	}

	var list api.ListResponse
	mustRequest(t, svc.nc, "tester.list", nil, &list)
	if list.Instances == nil {
		t.Errorf("list with an empty body = %+v, want instances", list)
	}

	var schema api.SchemaResponse
	mustRequest(t, svc.nc, "tester.schema", nil, &schema)
	if len(schema.Endpoints) != len(api.Endpoints) {
		t.Errorf("schema with an empty body lists %d endpoints, want %d", len(schema.Endpoints), len(api.Endpoints))
	}

	for _, subject := range []string{"tester.reset", "tester.list", "tester.schema"} {
		code, desc := rawServiceError(t, svc.nc, subject, []byte(`{"instance_id":"x"}`))
		if code != "001" || desc != "Invalid request: additional properties 'instance_id' not allowed" {
			t.Errorf("%s with a bad body = %q %q, want 001 and the unknown field", subject, code, desc)
		}
		code, desc = rawServiceError(t, svc.nc, subject, []byte("nope"))
		if code != "001" || !strings.HasPrefix(desc, "Invalid request: ") {
			t.Errorf("%s with a body that is not JSON = %q %q, want 001", subject, code, desc)
		}
	}

	code, desc := rawServiceError(t, svc.nc, "tester.destroy", nil)
	if code != "001" || !strings.HasPrefix(desc, "Invalid request: ") {
		t.Errorf("destroy with an empty body = %q %q, want 001", code, desc)
	}
}

// TestSchemaEndpoint proves tester.schema answers every endpoint, its own
// included, sorted by name, with its description and its request and
// response schemas inlined.
func TestSchemaEndpoint(t *testing.T) {
	svc := startTestService(t)

	var resp api.SchemaResponse
	mustRequest(t, svc.nc, "tester.schema", api.SchemaRequest{}, &resp)

	if resp.Type != api.TypeSchemaResponse {
		t.Errorf("type = %q, want %q", resp.Type, api.TypeSchemaResponse)
	}
	if len(resp.Endpoints) != len(api.Endpoints) {
		t.Fatalf("%d endpoints, want %d", len(resp.Endpoints), len(api.Endpoints))
	}

	var names []string
	for _, ep := range resp.Endpoints {
		names = append(names, ep.Name)
	}
	if !slices.IsSorted(names) {
		t.Errorf("endpoints not sorted by name: %v", names)
	}
	if !slices.Contains(names, "schema") {
		t.Errorf("endpoints %v do not include schema", names)
	}

	byName := map[string]api.EndpointSchema{}
	for _, ep := range resp.Endpoints {
		byName[ep.Name] = ep
	}
	for _, ep := range api.Endpoints {
		got, ok := byName[ep.Name]
		if !ok {
			t.Errorf("endpoint %s missing", ep.Name)
			continue
		}
		if got.Description != ep.Description {
			t.Errorf("%s description = %q, want %q", ep.Name, got.Description, ep.Description)
		}
		for _, side := range []struct {
			got json.RawMessage
			typ string
		}{{got.Request, ep.RequestType}, {got.Response, ep.ResponseType}} {
			schema, err := api.Schema(side.typ)
			if err != nil {
				t.Fatal(err)
			}
			var want bytes.Buffer
			err = json.Compact(&want, schema)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(side.got, want.Bytes()) {
				t.Errorf("%s schema of %s is not the embedded one inlined", ep.Name, side.typ)
			}
		}
	}
}

// responseSchemas compiles the response schema of every endpoint, by endpoint
// name.
func responseSchemas(t *testing.T) map[string]*jsonschema.Schema {
	t.Helper()

	c := jsonschema.NewCompiler()
	c.AssertFormat()
	// The create endpoints share a response type, which is added once.
	byType := map[string]*jsonschema.Schema{}
	schemas := map[string]*jsonschema.Schema{}
	for _, ep := range api.Endpoints {
		if byType[ep.ResponseType] == nil {
			schema, err := api.Schema(ep.ResponseType)
			if err != nil {
				t.Fatal(err)
			}
			doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
			if err != nil {
				t.Fatalf("schema of %s is not JSON: %v", ep.ResponseType, err)
			}
			url := ep.ResponseType + ".json"
			err = c.AddResource(url, doc)
			if err != nil {
				t.Fatalf("could not add schema of %s: %v", ep.ResponseType, err)
			}
			byType[ep.ResponseType], err = c.Compile(url)
			if err != nil {
				t.Fatalf("could not compile schema of %s: %v", ep.ResponseType, err)
			}
		}
		schemas[ep.Name] = byType[ep.ResponseType]
	}

	return schemas
}

// responseChecker sends requests to the tester endpoints and checks each
// answer against its endpoint's response schema and response type.
type responseChecker struct {
	t       *testing.T
	nc      *nats.Conn
	schemas map[string]*jsonschema.Schema
	types   map[string]string
	checked map[string]bool
}

// request sends req to the endpoint name, fails on an error answer, checks the
// answer, and unmarshals it into resp when resp is not nil.
func (c *responseChecker) request(name string, req any, resp any) {
	c.t.Helper()

	payload, err := json.Marshal(req)
	if err != nil {
		c.t.Fatalf("marshal %s: %v", name, err)
	}
	msg, err := c.nc.Request("tester."+name, payload, 30*time.Second)
	if err != nil {
		c.t.Fatalf("request %s: %v", name, err)
	}
	e := msg.Header.Get("Nats-Service-Error")
	if e != "" {
		c.t.Fatalf("request %s failed: %s", name, e)
	}

	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(msg.Data))
	if err != nil {
		c.t.Fatalf("%s response is not JSON: %v", name, err)
	}
	err = c.schemas[name].Validate(inst)
	if err != nil {
		c.t.Errorf("%s response %s does not match its schema: %v", name, msg.Data, err)
	}

	var typed struct {
		Type string `json:"type"`
	}
	err = json.Unmarshal(msg.Data, &typed)
	if err != nil {
		c.t.Fatalf("unmarshal %s response: %v", name, err)
	}
	if typed.Type != c.types[name] {
		c.t.Errorf("%s response type = %q, want %q", name, typed.Type, c.types[name])
	}
	c.checked[name] = true

	if resp != nil {
		err = json.Unmarshal(msg.Data, resp)
		if err != nil {
			c.t.Fatalf("unmarshal %s response: %v", name, err)
		}
	}
}

// TestResponsesMatchSchemas proves the answer of every endpoint matches its
// response schema and carries its response type, with arrays answered empty
// rather than null.
func TestResponsesMatchSchemas(t *testing.T) {
	fake := &fakeAgent{reports: []api.ShapingReport{{ID: "lost-ack-30"}}}
	svc := startTestService(t, withAgent(&fakeProvider{toxics: true, agent: fake}))

	c := &responseChecker{t: t, nc: svc.nc, schemas: responseSchemas(t), types: map[string]string{}, checked: map[string]bool{}}
	for _, ep := range api.Endpoints {
		c.types[ep.Name] = ep.ResponseType
	}

	// Before any instance exists the lists are empty.
	c.request("status", api.StatusRequest{}, nil)
	c.request("list", api.ListRequest{}, nil)

	var plain api.CreateResponse
	c.request("create.server", api.CreateServerRequest{}, &plain)
	name := plain.Servers[0].Name

	var traced api.CreateResponse
	c.request("create.server", api.CreateServerRequest{Trace: true}, &traced)

	var toxics api.CreateResponse
	c.request("create.cluster", api.CreateClusterRequest{Servers: 2, Toxics: true}, &toxics)

	c.request("create.super-cluster", api.CreateSuperClusterRequest{Servers: 2, Clusters: 2}, nil)

	c.request("stop.server", api.StopServerRequest{Name: name}, nil)
	c.request("start.server", api.StartServerRequest{Name: name}, nil)
	c.request("update.server", api.UpdateServerRequest{Name: name}, nil)
	c.request("reload.server", api.ReloadServerRequest{Name: name}, nil)
	c.request("stop.instance", api.StopInstanceRequest{InstanceID: plain.ID}, nil)
	c.request("start.instance", api.StartInstanceRequest{InstanceID: plain.ID}, nil)

	c.request("shape.set", api.ShapeSetRequest{InstanceID: traced.ID, Set: exampleShapingSet()}, nil)
	var report api.ShapeReportResponse
	c.request("shape.report", api.ShapeReportRequest{InstanceID: traced.ID}, &report)
	if len(report.Sets) != 1 || report.Sets[0].Firings == nil {
		t.Errorf("shape report = %+v, want the set with empty firings", report.Sets)
	}
	// A stored set with nil Rules is listed with its rules empty.
	c.request("shape.set", api.ShapeSetRequest{InstanceID: traced.ID, Set: api.ShapingSet{ID: "bare", ConnectionName: "^bare$"}}, nil)
	var listed api.ShapeListResponse
	c.request("shape.list", api.ShapeListRequest{InstanceID: traced.ID}, &listed)
	if len(listed.Sets) != 2 || listed.Sets[1].Rules == nil {
		t.Errorf("shape list = %+v, want both sets, the bare one with empty rules", listed.Sets)
	}
	c.request("shape.clear", api.ShapeClearRequest{InstanceID: traced.ID}, nil)

	toxic := api.Toxic{Name: "slow", Type: api.ToxicLatency, Attributes: map[string]any{"latency": 100}}
	c.request("toxic.report", api.ToxicReportRequest{InstanceID: toxics.ID}, nil)
	c.request("toxic.add", api.ToxicAddRequest{InstanceID: toxics.ID, Toxic: toxic}, nil)
	c.request("toxic.report", api.ToxicReportRequest{InstanceID: toxics.ID}, nil)
	c.request("toxic.remove", api.ToxicRemoveRequest{InstanceID: toxics.ID, Name: "slow"}, nil)
	c.request("toxic.clear", api.ToxicClearRequest{InstanceID: toxics.ID}, nil)

	c.request("status", api.StatusRequest{}, nil)
	c.request("status", api.StatusRequest{InstanceID: toxics.ID}, nil)
	c.request("list", api.ListRequest{}, nil)
	c.request("clock", api.ClockRequest{Extra: []string{"no.such.clock"}}, nil)
	c.request("schema", api.SchemaRequest{}, nil)
	c.request("destroy", api.DestroyRequest{InstanceID: plain.ID}, nil)
	c.request("reset", api.ResetRequest{}, nil)

	for _, ep := range api.Endpoints {
		if !c.checked[ep.Name] {
			t.Errorf("no response of %s was checked", ep.Name)
		}
	}
}
