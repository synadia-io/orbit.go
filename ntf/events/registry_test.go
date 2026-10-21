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
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// TestParseEventRoundTrip marshals an example of every type and parses it
// back into an equal value of the same struct.
func TestParseEventRoundTrip(t *testing.T) {
	for _, want := range examples() {
		t.Run(want.EventType()+"/"+want.EventAction(), func(t *testing.T) {
			data, err := json.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}

			got, err := ParseEvent(data)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.TypeOf(got) != reflect.TypeOf(want) {
				t.Fatalf("parsed as %T, want %T", got, want)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("parsed %+v, want %+v", got, want)
			}
		})
	}
}

// TestParseEventUnknown checks a type this package does not know, or no type
// at all, parses as an UnknownEvent holding the common fields and the whole
// message.
func TestParseEventUnknown(t *testing.T) {
	cases := map[string]string{
		"later type": `{"type":"io.synadia.ntf.event.v1.chaos_action","id":"abc","timestamp":"2026-10-15T12:30:45.123456789Z","instance_id":"inst","source":"chaos","source_seq":4,"scenario":"partition"}`,
		"no type":    `{"id":"abc","instance_id":"inst","source_seq":4}`,
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			e, err := ParseEvent([]byte(data))
			if err != nil {
				t.Fatal(err)
			}
			unknown, ok := e.(*UnknownEvent)
			if !ok {
				t.Fatalf("parsed as %T, want *UnknownEvent", e)
			}
			if unknown.ID != "abc" || unknown.InstanceID != "inst" || unknown.SourceSeq != 4 {
				t.Errorf("common fields not kept: %+v", unknown.Base)
			}
			if e.EventCategory() != "" || e.EventAction() != "" {
				t.Errorf("unknown event reports category %q and action %q", e.EventCategory(), e.EventAction())
			}

			out, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if string(out) != data {
				t.Errorf("marshaled %s, want %s", out, data)
			}
		})
	}
}

func TestParseEventInvalid(t *testing.T) {
	cases := map[string]string{
		"not json":   `{`,
		"null":       `null`,
		"bad field":  `{"type":"io.synadia.ntf.event.v1.node_action","source_seq":"one"}`,
		"bad common": `{"type":"io.synadia.ntf.event.v1.later","source_seq":"one"}`,
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseEvent([]byte(data))
			if !errors.Is(err, ErrInvalidEvent) {
				t.Errorf("got %v, want ErrInvalidEvent", err)
			}
		})
	}
}
