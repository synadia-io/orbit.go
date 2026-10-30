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

// JSONSchemaExtend opens the attributes object and adds the rules of the
// attributes each toxic type accepts.
func (Toxic) JSONSchemaExtend(s *invopop.Schema) {
	openObject(s, "attributes")
	s.AllOf = toxicAttributeRules(false)
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

// JSONSchemaExtend opens the where object.
func (SubjectMatch) JSONSchemaExtend(s *invopop.Schema) {
	openObject(s, "where")
}

// JSONSchemaExtend opens the json object.
func (PayloadMatch) JSONSchemaExtend(s *invopop.Schema) {
	openObject(s, "json")
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

// encodeSchema returns s as indented JSON, as the committed files hold it.
func encodeSchema(s *invopop.Schema) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	err := enc.Encode(s)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// reflectSchema returns the schema of type t, whose type id is typ, inlined
// with nothing in $defs. A request refuses properties it does not declare and
// a response allows them. Only the root gets the $id, the title and the const
// of its type property, since a Toxic has a type of its own.
func reflectSchema(t reflect.Type, typ string, request bool) ([]byte, error) {
	r := &invopop.Reflector{
		Anonymous:                  true,
		AllowAdditionalProperties:  !request,
		RequiredFromJSONSchemaTags: true,
		DoNotReference:             true,
		ExpandedStruct:             true,
	}
	s := r.ReflectFromType(t)

	p, err := schemaPath(typ)
	if err != nil {
		return nil, err
	}
	s.ID = invopop.ID(schemaBaseURL + p)
	s.Title = typ

	prop, ok := s.Properties.Get("type")
	if !ok {
		return nil, fmt.Errorf("%s has no type property", t)
	}
	prop.Const = typ

	return encodeSchema(s)
}

// generatedSchemas returns every schema file the types of Endpoints and the
// toxic attributes produce, by their path below this package's directory.
func generatedSchemas(t *testing.T) map[string][]byte {
	t.Helper()

	files := map[string][]byte{}
	add := func(rt reflect.Type, typ string, request bool) {
		p, err := schemaPath(typ)
		if err != nil {
			t.Fatal(err)
		}
		if files[p] != nil {
			return
		}

		data, err := reflectSchema(rt, typ, request)
		if err != nil {
			t.Fatalf("could not reflect %s: %v", typ, err)
		}
		files[p] = data
	}

	for _, ep := range Endpoints {
		add(ep.Request, ep.RequestType, true)
		add(ep.Response, ep.ResponseType, false)
	}

	fragment := &invopop.Schema{
		Version:     invopop.Version,
		ID:          invopop.ID(schemaBaseURL + toxicAttributesPath),
		Description: "The attributes each toxic type accepts, as one rule on the type and attributes of a toxic for each type",
		AllOf:       toxicAttributeRules(true),
	}
	data, err := encodeSchema(fragment)
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
			Instances: []InstanceSummary{{ID: "abc", Kind: "cluster", Cluster: "C_abc", Servers: 3, Created: created}},
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

func TestSchemaUnknownType(t *testing.T) {
	_, err := Schema("io.synadia.ntf.tester.v1.nope")
	if !errors.Is(err, ErrUnknownType) {
		t.Errorf("got %v, want ErrUnknownType", err)
	}
}
