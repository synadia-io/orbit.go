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

package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	invopop "github.com/invopop/jsonschema"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/synadia-io/orbit.go/ntf/schema/gen"
)

// updateSchemas rewrites the committed schemas under schemas/ntf/tester/v1
// from the Go types, instead of comparing with them.
var updateSchemas = flag.Bool("update", false, "rewrite the schemas under schemas/ntf/tester/v1")

const (
	// schemaDir is the directory of the committed schemas, below this
	// package's directory.
	schemaDir = "schemas/ntf/tester/v1"

	// schemaBaseURL is where this package's directory is served on orbit.go's
	// main branch; a schema's $id is its path below it.
	schemaBaseURL = "https://raw.githubusercontent.com/synadia-io/orbit.go/main/ntf/api/"

	// toxicAttributesPath is the path of the toxic attributes fragment.
	toxicAttributesPath = schemaDir + "/toxic_attributes.json"

	// regenerateCommand is the command that rewrites the committed schemas,
	// run from the ntf module's directory.
	regenerateCommand = "go test ./api -run TestSchemasCurrent -update"
)

// JSONSchema describes the two forms of a ShapingAction: the bare string of a
// drop or disconnect, and the object with one key of a stall or throttle.
func (ShapingAction) JSONSchema() *invopop.Schema {
	throttle := &invopop.Schema{
		Type:                 "object",
		Description:          "Limits the frame's direction to rate with a burst allowance",
		Properties:           invopop.NewProperties(),
		AdditionalProperties: invopop.FalseSchema,
	}
	throttle.Properties.Set("rate", &invopop.Schema{Type: "string", Description: "The rate, such as 50/s"})
	throttle.Properties.Set("burst", &invopop.Schema{Type: "integer", Description: "The burst allowance"})

	object := &invopop.Schema{
		Type:                 "object",
		Description:          "A stall or throttle, keyed by its kind",
		Properties:           invopop.NewProperties(),
		MinProperties:        new(uint64(1)),
		MaxProperties:        new(uint64(1)),
		AdditionalProperties: invopop.FalseSchema,
	}
	object.Properties.Set(ShapingStall, &invopop.Schema{Type: "string", Description: "Pauses the frame's direction for this long, a Go duration string"})
	object.Properties.Set(ShapingThrottle, throttle)

	return &invopop.Schema{
		AllOf: []*invopop.Schema{
			{
				If:   &invopop.Schema{Type: "string"},
				Then: &invopop.Schema{Type: "string", Description: "A drop or disconnect, by its kind"},
			},
			{
				If:   &invopop.Schema{Not: &invopop.Schema{Type: "string"}},
				Then: object,
			},
		},
	}
}

// JSONSchemaExtend opens the attributes object, lists the toxic types with
// what each does, and adds the rules of the attributes each type accepts.
func (Toxic) JSONSchemaExtend(s *invopop.Schema) {
	openObject(s, "attributes")
	describeToxicTypes(s)
	s.AllOf = toxicAttributeRules(false)
}

// describeToxicTypes gives the type property of s a oneOf of one const for
// each toxic type in ToxicAttributes, in the order of their names, with its
// description from ToxicDescriptions. An enum of the same types sits beside
// it, so an unknown type is refused naming every type rather than only the
// first const the oneOf tried.
func describeToxicTypes(s *invopop.Schema) {
	p, ok := s.Properties.Get("type")
	if !ok {
		return
	}

	for _, typ := range slices.Sorted(maps.Keys(ToxicAttributes)) {
		p.Enum = append(p.Enum, typ)
		p.OneOf = append(p.OneOf, &invopop.Schema{Const: typ, Description: ToxicDescriptions[typ]})
	}
}

// JSONSchemaExtend opens the attributes object. It keeps the rules of Toxic's,
// which ActiveToxic would otherwise inherit, out of the responses.
func (ActiveToxic) JSONSchemaExtend(s *invopop.Schema) {
	openObject(s, "attributes")
}

// JSONSchemaExtend opens the attributes object.
func (LinkToxic) JSONSchemaExtend(s *invopop.Schema) {
	openObject(s, "attributes")
}

// JSONSchemaExtend lets rules be null as well as an array, since a set with
// nil Rules marshals them as null. The items stay beside the anyOf rather
// than inside its array branch, so a bad rule is reported at its own path
// alone, not also as rules failing to be null.
func (ShapingSet) JSONSchemaExtend(s *invopop.Schema) {
	p, ok := s.Properties.Get("rules")
	if !ok {
		return
	}

	s.Properties.Set("rules", &invopop.Schema{
		AnyOf:       []*invopop.Schema{{Type: "null"}, {Type: p.Type}},
		Items:       p.Items,
		Description: p.Description,
	})
}

// JSONSchemaExtend describes the constraint of each capture in the where
// object: a scalar the capture must equal, or an object of at least one
// operator. Whether the capture exists and is an int, as gte and lte need, is
// left to the grammar.
func (SubjectMatch) JSONSchemaExtend(s *invopop.Schema) {
	operators := &invopop.Schema{
		Type:                 "object",
		Description:          "Operators the capture must satisfy, all of them",
		Properties:           invopop.NewProperties(),
		MinProperties:        new(uint64(1)),
		AdditionalProperties: invopop.FalseSchema,
	}
	operators.Properties.Set("gte", &invopop.Schema{Type: "number", Description: "The minimum value of the int capture"})
	operators.Properties.Set("lte", &invopop.Schema{Type: "number", Description: "The maximum value of the int capture"})
	operators.Properties.Set("in", &invopop.Schema{
		Type:        "array",
		Description: "The allowed values of the capture",
		MinItems:    new(uint64(1)),
		Items:       &invopop.Schema{OneOf: scalarSchemas()},
	})

	objectOf(s, "where", &invopop.Schema{OneOf: append(scalarSchemas(), operators)})
}

// JSONSchemaExtend lets each gjson path of the json object hold a scalar or
// null, the values the payload at that path is compared with.
func (PayloadMatch) JSONSchemaExtend(s *invopop.Schema) {
	objectOf(s, "json", &invopop.Schema{OneOf: append(scalarSchemas(), &invopop.Schema{Type: "null"})})
}

// scalarSchemas returns the schemas of a string, a number and a boolean, the
// scalars a where constraint compares a capture with and a payload json path
// its value with.
func scalarSchemas() []*invopop.Schema {
	return []*invopop.Schema{{Type: "string"}, {Type: "number"}, {Type: "boolean"}}
}

// objectOf makes property name of s, a map[string]any, an object whose
// properties match values.
func objectOf(s *invopop.Schema, name string, values *invopop.Schema) {
	p, ok := s.Properties.Get(name)
	if !ok {
		return
	}

	p.Type = "object"
	p.AdditionalProperties = values
}

// openObject makes property name of s, a map[string]any, an object that
// allows any property.
func openObject(s *invopop.Schema, name string) {
	p, ok := s.Properties.Get(name)
	if !ok {
		return
	}

	p.Type = "object"
	p.AdditionalProperties = invopop.TrueSchema
}

// toxicAttributeRules returns one if/then rule for each toxic type in
// ToxicAttributes, in the order of their names: when a toxic's type is that
// type, its attributes hold only that type's attributes, and the attributes
// without a default. With allowNull, the attributes of a type without a
// required attribute may also be null, as the agent reads null as none.
func toxicAttributeRules(allowNull bool) []*invopop.Schema {
	var rules []*invopop.Schema
	for _, typ := range slices.Sorted(maps.Keys(ToxicAttributes)) {
		attrs := &invopop.Schema{
			Type:                 "object",
			Properties:           invopop.NewProperties(),
			AdditionalProperties: invopop.FalseSchema,
		}
		for _, a := range ToxicAttributes[typ] {
			attrs.Properties.Set(a.Name, toxicAttributeSchema(a))
			if a.Default == nil {
				attrs.Required = append(attrs.Required, a.Name)
			}
		}

		then := &invopop.Schema{Properties: invopop.NewProperties()}
		if allowNull && len(attrs.Required) == 0 {
			then.Properties.Set("attributes", &invopop.Schema{AnyOf: []*invopop.Schema{{Type: "null"}, attrs}})
		} else {
			then.Properties.Set("attributes", attrs)
		}
		if len(attrs.Required) > 0 {
			then.Required = []string{"attributes"}
		}

		cond := &invopop.Schema{Properties: invopop.NewProperties(), Required: []string{"type"}}
		cond.Properties.Set("type", &invopop.Schema{Const: typ})

		rules = append(rules, &invopop.Schema{If: cond, Then: then})
	}

	return rules
}

// toxicAttributeSchema returns the schema of one toxic attribute: an integer
// from its minimum to its maximum, or a number between them for a fraction,
// with its default when it has one and its unit written out.
func toxicAttributeSchema(a ToxicAttribute) *invopop.Schema {
	s := &invopop.Schema{Description: a.Description}
	if a.Unit != ToxicUnitNone {
		s.Description += " Measured in " + string(a.Unit) + "."
	}

	minimum := json.Number(strconv.FormatInt(a.Minimum, 10))
	maximum := json.Number(strconv.FormatInt(a.Maximum, 10))
	if a.Fraction {
		s.Type = "number"
		s.ExclusiveMinimum = minimum
		s.ExclusiveMaximum = maximum
	} else {
		s.Type = "integer"
		s.Minimum = minimum
		s.Maximum = maximum
	}

	if a.Default != nil {
		s.Default = *a.Default
	}

	return s
}

// generatedSchemas returns every schema file the types of Endpoints and the
// toxic attributes produce, by their path below this package's directory.
func generatedSchemas(t *testing.T) map[string][]byte {
	t.Helper()

	files := map[string][]byte{}
	add := func(rt reflect.Type, typ string, reflectType func(reflect.Type, string, string, gen.Mapper) ([]byte, error)) {
		p, err := schemaPath(typ)
		if err != nil {
			t.Fatal(err)
		}
		if files[p] != nil {
			return
		}

		data, err := reflectType(rt, typ, schemaBaseURL+p, nil)
		if err != nil {
			t.Fatalf("could not reflect %s: %v", typ, err)
		}
		files[p] = data
	}

	for _, ep := range Endpoints {
		add(ep.Request, ep.RequestType, gen.Request)
		add(ep.Response, ep.ResponseType, gen.Response)
	}

	fragment := &invopop.Schema{
		Version:     invopop.Version,
		ID:          invopop.ID(schemaBaseURL + toxicAttributesPath),
		Description: "The attributes each toxic type accepts, as one rule for each toxic type",
		AllOf:       toxicAttributeRules(true),
	}
	data, err := gen.Encode(fragment)
	if err != nil {
		t.Fatalf("could not encode the toxic attributes: %v", err)
	}
	files[toxicAttributesPath] = data

	return files
}

// TestSchemasCurrent generates the schemas of every request and response and
// the toxic attributes fragment, and fails when the committed files differ
// from them. With -update it rewrites the files instead.
func TestSchemasCurrent(t *testing.T) {
	want := generatedSchemas(t)

	if *updateSchemas {
		err := os.MkdirAll(filepath.FromSlash(schemaDir), 0o755)
		if err != nil {
			t.Fatal(err)
		}
		for p, data := range want {
			err = os.WriteFile(filepath.FromSlash(p), data, 0o644)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	for _, p := range slices.Sorted(maps.Keys(want)) {
		got, err := os.ReadFile(filepath.FromSlash(p))
		if err != nil {
			t.Errorf("%s could not be read, run %q: %v", p, regenerateCommand, err)
			continue
		}
		if !bytes.Equal(got, want[p]) {
			t.Errorf("%s is stale, run %q", p, regenerateCommand)
		}
	}

	entries, err := os.ReadDir(filepath.FromSlash(schemaDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		p := path.Join(schemaDir, e.Name())
		if want[p] == nil {
			t.Errorf("%s is generated from no type, delete it", p)
		}
	}
}

// schemaValidator holds the compiled embedded schema of every type of
// Endpoints, by type id, compiled with santhosh-tekuri/jsonschema.
type schemaValidator map[string]*jsonschema.Schema

func newSchemaValidator(t *testing.T) schemaValidator {
	t.Helper()

	v := schemaValidator{}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	for _, ep := range Endpoints {
		for _, typ := range []string{ep.RequestType, ep.ResponseType} {
			if v[typ] != nil {
				continue
			}

			schema, err := Schema(typ)
			if err != nil {
				t.Fatal(err)
			}
			doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
			if err != nil {
				t.Fatalf("schema of %s is not JSON: %v", typ, err)
			}
			p, err := schemaPath(typ)
			if err != nil {
				t.Fatal(err)
			}
			err = c.AddResource(p, doc)
			if err != nil {
				t.Fatalf("could not add schema of %s: %v", typ, err)
			}
			v[typ], err = c.Compile(p)
			if err != nil {
				t.Fatalf("could not compile schema of %s: %v", typ, err)
			}
		}
	}

	return v
}

// validate marshals data and validates it against the schema of typ.
func (v schemaValidator) validate(data any, typ string) error {
	sch, ok := v[typ]
	if !ok {
		return fmt.Errorf("no schema for %s", typ)
	}

	j, err := json.Marshal(data)
	if err != nil {
		return err
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(j))
	if err != nil {
		return err
	}

	return sch.Validate(inst)
}

// examples returns an example of every request and response type, by its
// type id, with its type set.
func examples() map[string]any {
	created := time.Date(2026, 10, 15, 12, 30, 45, 123456789, time.UTC)
	toxicity := 0.5
	timeout := 0.5
	link := Link{Name: "route:n1->n2", Kind: ToxicKindRoute, From: "n1", To: "n2", Proxy: "127.0.0.1:21000", Upstream: "127.0.0.1:20001"}
	toxic := Toxic{
		Name:       "slow-route",
		Selector:   ToxicSelector{Kind: ToxicKindRoute, From: "n1", To: "n2", Direction: ToxicDirectionTo},
		Type:       ToxicLatency,
		Attributes: map[string]any{"latency": 250.0, "jitter": 10.0},
		Toxicity:   &toxicity,
	}
	active := ActiveToxic{Toxic: toxic, Links: []string{"route:n1->n2"}, Since: created}
	server := ManagedServer{Name: "n1", Cluster: "C1", Port: 20000, Ports: map[string]int{"cluster": 20001}, Advertise: "10.0.0.1", Running: true, Version: "2.12.0"}
	results := []ServerStateResult{{Name: "n1", Running: true}, {Name: "n2", Error: "port in use"}}

	return map[string]any{
		TypeCreateServerRequest: CreateServerRequest{
			Type:        TypeCreateServerRequest,
			JetStream:   true,
			Description: "leader election test",
			Snippets:    map[string]string{"accounts": "accounts {}"},
			TLS:         &TLSOptions{Mode: TLSModeMutual, SANs: []string{"localhost"}, HandshakeFirst: true, Timeout: 0.5},
			Trace:       true,
		},
		TypeCreateClusterRequest:      CreateClusterRequest{Type: TypeCreateClusterRequest, Servers: 3, JetStream: true, Toxics: true},
		TypeCreateSuperClusterRequest: CreateSuperClusterRequest{Type: TypeCreateSuperClusterRequest, Servers: 3, Clusters: 2, Proxy: true},
		TypeCreateResponse: CreateResponse{
			Type:    TypeCreateResponse,
			ID:      "abc",
			Kind:    "cluster",
			Servers: []*ManagedServer{&server},
			TLS:     &TLSMaterial{CAPEM: "ca", ClientCertPEM: "cert", ClientKeyPEM: "key"},
			Links:   []Link{link},
		},
		TypeStopServerRequest:     StopServerRequest{Type: TypeStopServerRequest, Name: "n1"},
		TypeStopServerResponse:    StopServerResponse{Type: TypeStopServerResponse, Shutdown: true},
		TypeStopInstanceRequest:   StopInstanceRequest{Type: TypeStopInstanceRequest, InstanceID: "abc"},
		TypeStopInstanceResponse:  StopInstanceResponse{Type: TypeStopInstanceResponse, Servers: results},
		TypeStartServerRequest:    StartServerRequest{Type: TypeStartServerRequest, Name: "n1"},
		TypeStartServerResponse:   StartServerResponse{Type: TypeStartServerResponse, Started: true},
		TypeStartInstanceRequest:  StartInstanceRequest{Type: TypeStartInstanceRequest, InstanceID: "abc"},
		TypeStartInstanceResponse: StartInstanceResponse{Type: TypeStartInstanceResponse, Servers: results},
		TypeUpdateServerRequest:   UpdateServerRequest{Type: TypeUpdateServerRequest, Name: "n1", Snippets: map[string]string{"top": "debug: true"}, TLSTimeout: &timeout},
		TypeUpdateServerResponse:  UpdateServerResponse{Type: TypeUpdateServerResponse, Updated: true},
		TypeReloadServerRequest:   ReloadServerRequest{Type: TypeReloadServerRequest, Name: "n1"},
		TypeReloadServerResponse:  ReloadServerResponse{Type: TypeReloadServerResponse, Reloaded: true},
		TypeShapeSetRequest: ShapeSetRequest{
			Type:       TypeShapeSetRequest,
			InstanceID: "abc",
			Set: ShapingSet{
				ID:             "slow",
				ConnectionName: "^producer$",
				Server:         "n1",
				Rules: []ShapingRule{
					{
						ID: "stall-pub",
						Match: ShapingMatch{
							Direction: "to_server",
							Verb:      []string{"PUB"},
							Subject:   &SubjectMatch{Grammar: "orders.{id}", Where: map[string]any{"id": "1"}},
							Header:    &HeaderMatch{Name: "Nats-Msg-Id"},
							Payload:   &PayloadMatch{JSON: map[string]any{"order.id": 1.0}},
						},
						Action: ShapingAction{Kind: ShapingStall, Duration: "3s"},
						Nth:    2,
					},
					{ID: "throttle-msg", Match: ShapingMatch{Verb: []string{"MSG"}}, Action: ShapingAction{Kind: ShapingThrottle, Rate: "50/s", Burst: 10}},
					{ID: "drop-sub", Match: ShapingMatch{Verb: []string{"SUB"}, SID: "1"}, Action: ShapingAction{Kind: ShapingDrop}, Limit: 1},
				},
			},
		},
		TypeShapeSetResponse:   ShapeSetResponse{Type: TypeShapeSetResponse, Set: "slow"},
		TypeShapeClearRequest:  ShapeClearRequest{Type: TypeShapeClearRequest, InstanceID: "abc", Set: "slow"},
		TypeShapeClearResponse: ShapeClearResponse{Type: TypeShapeClearResponse, Cleared: []string{"slow"}},
		TypeShapeReportRequest: ShapeReportRequest{Type: TypeShapeReportRequest, InstanceID: "abc"},
		TypeShapeReportResponse: ShapeReportResponse{
			Type: TypeShapeReportResponse,
			Sets: []ShapingReport{{
				ID:      "slow",
				Firings: []ShapingFiring{{ConnectionUUID: "c0ffee", ClientName: "producer", FrameID: "c0ffee-12", Rule: "stall-pub", Action: ShapingStall, Time: created}},
				Servers: map[string]int{"n1": 1},
			}},
		},
		TypeShapeListRequest: ShapeListRequest{Type: TypeShapeListRequest, InstanceID: "abc", Set: "slow"},
		TypeShapeListResponse: ShapeListResponse{
			Type: TypeShapeListResponse,
			Sets: []ShapingSet{{
				ID:             "slow",
				ConnectionName: "^producer$",
				Server:         "n1",
				Rules:          []ShapingRule{{ID: "drop-sub", Match: ShapingMatch{Verb: []string{"SUB"}, SID: "1"}, Action: ShapingAction{Kind: ShapingDrop}, Limit: 1}},
			}},
		},
		TypeToxicAddRequest:     ToxicAddRequest{Type: TypeToxicAddRequest, InstanceID: "abc", Toxic: toxic},
		TypeToxicAddResponse:    ToxicAddResponse{Type: TypeToxicAddResponse, Toxic: active},
		TypeToxicRemoveRequest:  ToxicRemoveRequest{Type: TypeToxicRemoveRequest, InstanceID: "abc", Name: "slow-route"},
		TypeToxicRemoveResponse: ToxicRemoveResponse{Type: TypeToxicRemoveResponse, Removed: "slow-route"},
		TypeToxicClearRequest:   ToxicClearRequest{Type: TypeToxicClearRequest, InstanceID: "abc"},
		TypeToxicClearResponse:  ToxicClearResponse{Type: TypeToxicClearResponse, Cleared: []string{"slow-route"}},
		TypeToxicReportRequest:  ToxicReportRequest{Type: TypeToxicReportRequest, InstanceID: "abc"},
		TypeToxicReportResponse: ToxicReportResponse{
			Type: TypeToxicReportResponse,
			Links: []LinkToxics{{
				Link:   link,
				Toxics: []LinkToxic{{Name: "slow-route", Type: ToxicLatency, Direction: ToxicDirectionTo, Attributes: map[string]any{"latency": 250.0}, Toxicity: 0.5, Since: created}},
			}},
			Toxics: []ActiveToxic{active},
		},
		TypeStatusRequest: StatusRequest{Type: TypeStatusRequest, InstanceID: "abc"},
		TypeStatusResponse: StatusResponse{
			Type: TypeStatusResponse,
			Instances: []InstanceStatus{{
				ID:                "abc",
				Kind:              "cluster",
				Servers:           []ManagedServer{server},
				ShapingSets:       []string{"slow"},
				Backend:           "inprocess",
				Capabilities:      []string{"kill"},
				Agent:             "inprocess",
				AgentCapabilities: []string{"capture", "toxics"},
				Links:             []Link{link},
				Toxics:            []ActiveToxic{active},
				EventsSeq:         7,
			}},
		},
		TypeResetRequest:    ResetRequest{Type: TypeResetRequest},
		TypeResetResponse:   ResetResponse{Type: TypeResetResponse, Shutdown: true},
		TypeDestroyRequest:  DestroyRequest{Type: TypeDestroyRequest, InstanceID: "abc"},
		TypeDestroyResponse: DestroyResponse{Type: TypeDestroyResponse, Destroyed: true},
		TypeListRequest:     ListRequest{Type: TypeListRequest},
		TypeListResponse: ListResponse{
			Type:      TypeListResponse,
			Instances: []InstanceSummary{{ID: "abc", Kind: "cluster", Cluster: "C_abc", Servers: 3, Created: created, JetStream: true, Trace: true, Toxics: true}},
		},
		TypeClockRequest: ClockRequest{Type: TypeClockRequest, Extra: []string{"workload.clock"}},
		TypeClockResponse: ClockResponse{
			Type: TypeClockResponse,
			Participants: []ClockParticipant{
				{Kind: ClockService, Reference: true},
				{Kind: ClockAgent, InstanceID: "abc", SharedClock: true},
				{Kind: ClockExtra, Subject: "workload.clock", Offset: -2 * time.Millisecond, RoundTrip: time.Millisecond, Bound: 500 * time.Microsecond},
			},
		},
		TypeSchemaRequest: SchemaRequest{Type: TypeSchemaRequest},
		TypeSchemaResponse: SchemaResponse{
			Type: TypeSchemaResponse,
			Endpoints: []EndpointSchema{{
				Name:        "list",
				Description: "Lists every instance",
				Request:     json.RawMessage(`{"type":"object"}`),
				Response:    json.RawMessage(`{"type":"object"}`),
			}},
		},
	}
}

// TestExamplesValidate marshals an example of every type and validates it
// against its embedded schema, so the Go types and the schemas cannot drift.
// It also checks each schema's $id, title and type const against the type
// id, and that every type of Endpoints has an example.
func TestExamplesValidate(t *testing.T) {
	v := newSchemaValidator(t)
	all := examples()

	for _, typ := range slices.Sorted(maps.Keys(all)) {
		t.Run(typ, func(t *testing.T) {
			err := v.validate(all[typ], typ)
			if err != nil {
				t.Fatalf("example does not validate: %v", err)
			}

			schema, err := Schema(typ)
			if err != nil {
				t.Fatal(err)
			}
			var head struct {
				ID         string `json:"$id"`
				Title      string `json:"title"`
				Properties struct {
					Type struct {
						Const string `json:"const"`
					} `json:"type"`
				} `json:"properties"`
			}
			err = json.Unmarshal(schema, &head)
			if err != nil {
				t.Fatal(err)
			}

			name := strings.TrimPrefix(typ, "io.synadia.ntf.tester.v1.")
			wantID := schemaBaseURL + schemaDir + "/" + name + ".json"
			if head.ID != wantID {
				t.Errorf("$id is %q, want %q", head.ID, wantID)
			}
			if head.Title != typ {
				t.Errorf("title is %q, want %q", head.Title, typ)
			}
			if head.Properties.Type.Const != typ {
				t.Errorf("type const is %q, want %q", head.Properties.Type.Const, typ)
			}
		})
	}

	for _, ep := range Endpoints {
		for _, typ := range []string{ep.RequestType, ep.ResponseType} {
			_, ok := all[typ]
			if !ok {
				t.Errorf("no example of %s", typ)
			}
		}
	}

	fragment, err := Schema(TypeToxicAttributes)
	if err != nil {
		t.Fatal(err)
	}
	var head struct {
		ID string `json:"$id"`
	}
	err = json.Unmarshal(fragment, &head)
	if err != nil {
		t.Fatal(err)
	}
	wantID := schemaBaseURL + toxicAttributesPath
	if head.ID != wantID {
		t.Errorf("toxic attributes $id is %q, want %q", head.ID, wantID)
	}
}

// TestWhereSchema proves the schema of a where constraint accepts a scalar
// and an object of operators, and refuses an empty object and one with an
// unknown operator.
func TestWhereSchema(t *testing.T) {
	v := newSchemaValidator(t)

	cases := []struct {
		name  string
		where any
		valid bool
	}{
		{"string", "1", true},
		{"number", 30.0, true},
		{"boolean", true, true},
		{"operators", map[string]any{"gte": 1.0, "lte": 10.0, "in": []any{"1", 2.0, true}}, true},
		{"empty object", map[string]any{}, false},
		{"unknown operator", map[string]any{"gt": 1.0}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := ShapeSetRequest{
				Type:       TypeShapeSetRequest,
				InstanceID: "abc",
				Set: ShapingSet{
					ID:             "s",
					ConnectionName: "c",
					Rules: []ShapingRule{{
						ID:     "r",
						Match:  ShapingMatch{Subject: &SubjectMatch{Grammar: "orders.{id}", Where: map[string]any{"id": tc.where}}},
						Action: ShapingAction{Kind: ShapingDrop},
					}},
				},
			}

			err := v.validate(req, TypeShapeSetRequest)
			if tc.valid && err != nil {
				t.Errorf("where %v refused: %v", tc.where, err)
			}
			if !tc.valid && err == nil {
				t.Errorf("where %v accepted", tc.where)
			}
		})
	}
}

func TestPayloadJSONSchema(t *testing.T) {
	v := newSchemaValidator(t)

	cases := []struct {
		name  string
		value any
		valid bool
	}{
		{"string", "1", true},
		{"number", 30.0, true},
		{"boolean", false, true},
		{"null", nil, true},
		{"array", []any{1.0}, false},
		{"object", map[string]any{"a": 1.0}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := ShapeSetRequest{
				Type:       TypeShapeSetRequest,
				InstanceID: "abc",
				Set: ShapingSet{
					ID:             "s",
					ConnectionName: "c",
					Rules: []ShapingRule{{
						ID:     "r",
						Match:  ShapingMatch{Payload: &PayloadMatch{JSON: map[string]any{"order.id": tc.value}}},
						Action: ShapingAction{Kind: ShapingDrop},
					}},
				},
			}

			err := v.validate(req, TypeShapeSetRequest)
			if tc.valid && err != nil {
				t.Errorf("payload json %v refused: %v", tc.value, err)
			}
			if !tc.valid && err == nil {
				t.Errorf("payload json %v accepted", tc.value)
			}
		})
	}
}

// TestToxicTypeOneOf proves the type of a toxic in the published toxic.add
// request has one oneOf entry for each type in ToxicAttributes, with its
// description, and none for a type not in it.
func TestToxicTypeOneOf(t *testing.T) {
	schema, err := Schema(TypeToxicAddRequest)
	if err != nil {
		t.Fatal(err)
	}

	var doc struct {
		Properties struct {
			Toxic struct {
				Properties struct {
					Type struct {
						OneOf []struct {
							Const       string `json:"const"`
							Description string `json:"description"`
						} `json:"oneOf"`
					} `json:"type"`
				} `json:"properties"`
			} `json:"toxic"`
		} `json:"properties"`
	}
	err = json.Unmarshal(schema, &doc)
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]string{}
	for _, entry := range doc.Properties.Toxic.Properties.Type.OneOf {
		_, ok := ToxicAttributes[entry.Const]
		if !ok {
			t.Errorf("oneOf entry %q is not a type in ToxicAttributes", entry.Const)
		}
		got[entry.Const] = entry.Description
	}

	for typ := range ToxicAttributes {
		desc, ok := got[typ]
		if !ok {
			t.Errorf("toxic type %q has no oneOf entry", typ)
			continue
		}
		if desc == "" || desc != ToxicDescriptions[typ] {
			t.Errorf("oneOf entry %q has description %q, want %q", typ, desc, ToxicDescriptions[typ])
		}
	}
}

// TestToxicTypeSchema proves the schema of a toxic.add request accepts a known
// toxic type and refuses an unknown or empty one.
func TestToxicTypeSchema(t *testing.T) {
	v := newSchemaValidator(t)

	cases := []struct {
		typ   string
		valid bool
	}{
		{ToxicLatency, true},
		{ToxicCut, true},
		{"packet_loss", false},
		{"", false},
	}
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			req := ToxicAddRequest{
				Type:       TypeToxicAddRequest,
				InstanceID: "abc",
				Toxic:      Toxic{Name: "x", Type: tc.typ},
			}

			err := v.validate(req, TypeToxicAddRequest)
			if tc.valid && err != nil {
				t.Errorf("type %q refused: %v", tc.typ, err)
			}
			if !tc.valid && err == nil {
				t.Errorf("type %q accepted", tc.typ)
			}
		})
	}
}

func TestSchemaUnknownType(t *testing.T) {
	_, err := Schema("io.synadia.ntf.tester.v1.nope")
	if !errors.Is(err, ErrUnknownType) {
		t.Errorf("got %v, want ErrUnknownType", err)
	}
}

// TestCreateRequestNamesSchema proves the create schemas accept the name
// fields and a cluster list in place of the counts, and refuse a name that
// starts with other than a letter, holds other characters, or is too long:
// 32 characters for a server and 28 for a cluster.
func TestCreateRequestNamesSchema(t *testing.T) {
	v := newSchemaValidator(t)

	long := func(n int) string { return strings.Repeat("a", n) }

	tests := []struct {
		name  string
		typ   string
		req   any
		valid bool
	}{
		{"named server", TypeCreateServerRequest, CreateServerRequest{Name: "solo"}, true},
		{"server name of 32", TypeCreateServerRequest, CreateServerRequest{Name: long(32)}, true},
		{"server name of 33", TypeCreateServerRequest, CreateServerRequest{Name: long(33)}, false},
		{"server name starting with a digit", TypeCreateServerRequest, CreateServerRequest{Name: "1a"}, false},
		{"server name starting with -", TypeCreateServerRequest, CreateServerRequest{Name: "-a"}, false},
		{"server name with a dot", TypeCreateServerRequest, CreateServerRequest{Name: "a.b"}, false},
		{"cluster of counts", TypeCreateClusterRequest, CreateClusterRequest{Servers: 3}, true},
		{"cluster of names", TypeCreateClusterRequest, CreateClusterRequest{ClusterName: "east", ServerNames: []string{"a_1", "B-2"}}, true},
		{"cluster without a size", TypeCreateClusterRequest, CreateClusterRequest{ClusterName: "east"}, true},
		{"cluster name of 28", TypeCreateClusterRequest, CreateClusterRequest{Servers: 2, ClusterName: long(28)}, true},
		{"cluster name of 29", TypeCreateClusterRequest, CreateClusterRequest{Servers: 2, ClusterName: long(29)}, false},
		{"cluster of one name", TypeCreateClusterRequest, CreateClusterRequest{ServerNames: []string{"a"}}, false},
		{"cluster server name of 33", TypeCreateClusterRequest, CreateClusterRequest{ServerNames: []string{"a", long(33)}}, false},
		{"super-cluster of counts", TypeCreateSuperClusterRequest, CreateSuperClusterRequest{Servers: 2, Clusters: 2}, true},
		{"super-cluster of a list", TypeCreateSuperClusterRequest, CreateSuperClusterRequest{ClusterList: []ClusterSpec{{Name: "lon", Servers: 5}, {Name: "nyc", ServerNames: []string{"n1"}}, {Servers: 1}}}, true},
		{"super-cluster of one cluster", TypeCreateSuperClusterRequest, CreateSuperClusterRequest{ClusterList: []ClusterSpec{{Servers: 2}}}, false},
		{"listed cluster name starting with a digit", TypeCreateSuperClusterRequest, CreateSuperClusterRequest{ClusterList: []ClusterSpec{{Name: "1lon", Servers: 2}, {Servers: 2}}}, false},
		{"listed cluster name of 29", TypeCreateSuperClusterRequest, CreateSuperClusterRequest{ClusterList: []ClusterSpec{{Name: long(29), Servers: 2}, {Servers: 2}}}, false},
		{"listed cluster of 21", TypeCreateSuperClusterRequest, CreateSuperClusterRequest{ClusterList: []ClusterSpec{{Servers: 21}, {Servers: 2}}}, false},
		{"listed server name of 33", TypeCreateSuperClusterRequest, CreateSuperClusterRequest{ClusterList: []ClusterSpec{{ServerNames: []string{long(33)}}, {Servers: 2}}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.validate(tt.req, tt.typ)
			if tt.valid && err != nil {
				t.Errorf("refused: %v", err)
			}
			if !tt.valid && err == nil {
				t.Error("accepted, want a refusal")
			}
		})
	}
}

// TestCreateRequestsWithoutNamesSendNoNewFields proves a create that uses
// none of the name fields or the cluster list sends none of them, nor any
// other field a service from before them would refuse, so it still validates
// against that service's schemas, which refuse fields they do not declare.
func TestCreateRequestsWithoutNamesSendNoNewFields(t *testing.T) {
	tests := []struct {
		req  any
		want []string
	}{
		{CreateServerRequest{JetStream: true}, []string{"jetstream"}},
		{CreateClusterRequest{Servers: 3}, []string{"jetstream", "servers"}},
		{CreateSuperClusterRequest{Servers: 2, Clusters: 3}, []string{"clusters", "jetstream", "servers"}},
	}

	for _, tt := range tests {
		data, err := json.Marshal(tt.req)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		err = json.Unmarshal(data, &fields)
		if err != nil {
			t.Fatal(err)
		}
		got := slices.Sorted(maps.Keys(fields))
		if !slices.Equal(got, tt.want) {
			t.Errorf("%T sends %v, want %v", tt.req, got, tt.want)
		}
	}
}
