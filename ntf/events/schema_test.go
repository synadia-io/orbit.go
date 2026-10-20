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

package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nuid"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// schemaValidator is a StructValidator over the embedded schemas of the event
// and clock types, compiled with santhosh-tekuri/jsonschema.
type schemaValidator map[string]*jsonschema.Schema

func newSchemaValidator(t *testing.T) schemaValidator {
	t.Helper()

	kinds := slices.Collect(maps.Keys(registry))
	kinds = slices.AppendSeq(kinds, maps.Keys(clockTypes))

	v := schemaValidator{}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	for _, kind := range kinds {
		schema, err := Schema(kind)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
		if err != nil {
			t.Fatalf("schema of %s is not JSON: %v", kind, err)
		}
		path, err := schemaPath(kind)
		if err != nil {
			t.Fatal(err)
		}
		err = c.AddResource(path, doc)
		if err != nil {
			t.Fatalf("could not add schema of %s: %v", kind, err)
		}
		v[kind], err = c.Compile(path)
		if err != nil {
			t.Fatalf("could not compile schema of %s: %v", kind, err)
		}
	}
	return v
}

func (v schemaValidator) ValidateStruct(data any, schemaType string) (bool, []string) {
	sch, ok := v[schemaType]
	if !ok {
		return false, []string{"no schema for " + schemaType}
	}

	j, err := json.Marshal(data)
	if err != nil {
		return false, []string{err.Error()}
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(j))
	if err != nil {
		return false, []string{err.Error()}
	}

	err = sch.Validate(inst)
	if err != nil {
		return false, []string{err.Error()}
	}
	return true, nil
}

func base(source Source, seq uint64) Base {
	return Base{
		ID:         nuid.Next(),
		Timestamp:  time.Date(2026, 10, 15, 12, 30, 45, 123456789, time.UTC),
		InstanceID: nuid.Next(),
		Source:     source,
		SourceSeq:  seq,
	}
}

// examples returns an example of every type, with Type stamped from the
// event's own type id as a publisher does.
func examples() []Event {
	all := []Event{
		&InstanceAction{Base: base(SourceService, 1), Action: InstanceCreated, Kind: "cluster", Backend: "inprocess"},
		&InstanceAction{Base: base(SourceService, 9), Action: InstanceDestroyed, Reason: ReasonReset},
		&NodeAction{Base: base(SourceService, 2), Action: NodeStopped, Node: "n1", ServerName: "cluster-n1"},
		&ShapingAction{Base: base(SourceAgent, 1), Action: ShapingApplied, Set: "slow"},
		&ShapingAction{Base: base(SourceAgent, 6), Action: ShapingApplied, Set: "drop-n3", Server: "cluster-n3"},
		&ShapingFired{
			Base:           base(SourceAgent, 2),
			Set:            "slow",
			Rule:           "stall-pub",
			RuleAction:     "stall",
			Wait:           3 * time.Second,
			ConnectionUUID: "c0ffee",
			ClientName:     "producer",
			FrameID:        "c0ffee-12",
			ServerName:     "cluster-n1",
		},
		// An agent from before server_name publishes none.
		&ShapingFired{
			Base:           base(SourceAgent, 7),
			Set:            "slow",
			Rule:           "drop-pub",
			RuleAction:     "drop",
			ConnectionUUID: "c0ffee",
			ClientName:     "producer",
			FrameID:        "c0ffee-13",
		},
		&CaptureStored{
			Base:           base(SourceAgent, 3),
			Bucket:         "TRACES",
			Object:         "producer-c0ffee",
			ConnectionUUID: "c0ffee",
			ClientName:     "producer",
			ServerName:     "cluster-n1",
			Shaped:         true,
		},
		&ToxicAction{
			Base:       base(SourceAgent, 4),
			Action:     ToxicAdded,
			Name:       "slow-route",
			Selector:   ToxicSelector{Kind: "route", From: "n1", To: "n2", Direction: "to"},
			ToxicType:  "latency",
			Attributes: map[string]any{"latency": 250.0, "jitter": 0.0},
			Toxicity:   0.5,
			Links:      []string{"route:n1->n2", "route:n2->n1"},
		},
		&ToxicAction{
			Base:      base(SourceAgent, 5),
			Action:    ToxicCleared,
			Name:      "unplug-n3",
			Selector:  ToxicSelector{Node: "n3", Direction: "both"},
			ToxicType: "cut",
			Toxicity:  1,
			Links:     []string{"route:n1->n3", "route:n3->n1", "client:n3"},
		},
	}
	for _, e := range all {
		e.EventBase().Type = e.EventType()
	}
	return all
}

// TestExamplesValidate marshals an example of every type and validates it
// against its embedded schema, so the Go types and the schemas cannot drift.
// It also checks each schema's title, type const and $id against the type id.
func TestExamplesValidate(t *testing.T) {
	v := newSchemaValidator(t)

	covered := map[string]bool{}
	for _, e := range examples() {
		t.Run(e.EventType()+"/"+e.EventAction(), func(t *testing.T) {
			covered[e.EventType()] = true

			err := Validate(e, v)
			if err != nil {
				t.Fatalf("example does not validate: %v", err)
			}

			schema, err := Schema(e.EventType())
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

			name := strings.TrimPrefix(e.EventType(), "io.synadia.ntf.event.v1.")
			wantID := "https://raw.githubusercontent.com/synadia-io/orbit.go/main/ntf/events/schemas/ntf/event/v1/" + name + ".json"
			if head.ID != wantID {
				t.Errorf("$id is %q, want %q", head.ID, wantID)
			}
			if head.Title != e.EventType() {
				t.Errorf("title is %q, want %q", head.Title, e.EventType())
			}
			if head.Properties.Type.Const != e.EventType() {
				t.Errorf("type const is %q, want %q", head.Properties.Type.Const, e.EventType())
			}
		})
	}

	for kind := range registry {
		if !covered[kind] {
			t.Errorf("no example of %s", kind)
		}
	}
}

// TestSchemasRefuseAndAllow checks the schemas are not vacuous: a wrong
// action or a missing required field is refused, while an unknown extra field
// is allowed.
func TestSchemasRefuseAndAllow(t *testing.T) {
	v := newSchemaValidator(t)

	node := &NodeAction{Base: base(SourceService, 1), Action: "exploded", Node: "n1", ServerName: "n1"}
	node.Type = TypeNodeAction
	err := Validate(node, v)
	if !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("unknown node action: got %v, want ErrInvalidEvent", err)
	}

	created := &InstanceAction{Base: base(SourceService, 1), Action: InstanceCreated}
	created.Type = TypeInstanceAction
	err = Validate(created, v)
	if !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("created without kind and backend: got %v, want ErrInvalidEvent", err)
	}

	zero := &ShapingAction{Base: base(SourceAgent, 0), Action: ShapingCleared, Set: "s"}
	zero.Type = TypeShapingAction
	err = Validate(zero, v)
	if !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("source_seq 0: got %v, want ErrInvalidEvent", err)
	}

	extra := map[string]any{
		"type":        TypeShapingAction,
		"id":          nuid.Next(),
		"timestamp":   "2026-10-15T12:30:45.123456789Z",
		"instance_id": nuid.Next(),
		"source":      "agent",
		"source_seq":  1,
		"action":      "applied",
		"set":         "s",
		"added_later": true,
	}
	ok, errs := v.ValidateStruct(extra, TypeShapingAction)
	if !ok {
		t.Errorf("extra field refused: %v", errs)
	}
}

func TestValidateNeedsValidator(t *testing.T) {
	err := Validate(examples()[0], nil)
	if !errors.Is(err, ErrNoValidator) {
		t.Errorf("got %v, want ErrNoValidator", err)
	}
}

func TestSchemaUnknownType(t *testing.T) {
	_, err := Schema("io.synadia.ntf.event.v1.nope")
	if !errors.Is(err, ErrUnknownType) {
		t.Errorf("got %v, want ErrUnknownType", err)
	}
}
