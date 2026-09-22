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
	"encoding/json"
	"reflect"
	"testing"
)

// TestShapingActionJSON proves every action kind reads from the form the design
// writes it in and marshals back to that same form.
func TestShapingActionJSON(t *testing.T) {
	cases := []struct {
		name   string
		json   string
		action ShapingAction
	}{
		{"drop", `"drop"`, ShapingAction{Kind: ShapingDrop}},
		{"disconnect", `"disconnect"`, ShapingAction{Kind: ShapingDisconnect}},
		{"stall", `{"stall":"3s"}`, ShapingAction{Kind: ShapingStall, Duration: "3s"}},
		{"throttle", `{"throttle":{"rate":"50/s","burst":10}}`, ShapingAction{Kind: ShapingThrottle, Rate: "50/s", Burst: 10}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got ShapingAction
			if err := json.Unmarshal([]byte(tc.json), &got); err != nil {
				t.Fatalf("unmarshal %s: %v", tc.json, err)
			}
			if got != tc.action {
				t.Errorf("unmarshal %s = %+v, want %+v", tc.json, got, tc.action)
			}

			out, err := json.Marshal(tc.action)
			if err != nil {
				t.Fatalf("marshal %+v: %v", tc.action, err)
			}
			if string(out) != tc.json {
				t.Errorf("marshal %+v = %s, want %s", tc.action, out, tc.json)
			}
		})
	}
}

// TestShapingActionRejectsUnknownObject proves an object action must be keyed by
// stall or throttle and hold exactly one key.
func TestShapingActionRejectsUnknownObject(t *testing.T) {
	for _, in := range []string{`{"pause":"3s"}`, `{"stall":"3s","throttle":{}}`, `{}`, `{"stall":3}`, `42`} {
		var a ShapingAction
		if err := json.Unmarshal([]byte(in), &a); err == nil {
			t.Errorf("unmarshal %s succeeded as %+v, want an error", in, a)
		}
	}
}

// exampleSet is the design's lost-ack-30 rule file.
const exampleSet = `{
  "id": "lost-ack-30",
  "connection_name": "^adr50-fast-lostack$",
  "rules": [
    {
      "id": "drop-ack-30",
      "match": {
        "direction": "from_server",
        "verb": ["MSG"],
        "subject": {"grammar": "{prefix:rest}.{flow:int}.{gap}.{seq:int}.{op:int}.$FI",
                    "where": {"seq": 30}},
        "payload": {"json": {"type": "ack"}}
      },
      "action": "drop",
      "limit": 1
    },
    {
      "id": "stall-pub-100",
      "match": {"direction": "to_server", "verb": ["PUB", "HPUB"]},
      "nth": 100,
      "action": {"stall": "3s"}
    },
    {
      "id": "slow-link",
      "match": {"direction": "to_server"},
      "action": {"throttle": {"rate": "50/s", "burst": 10}}
    }
  ]
}`

// TestShapingSetExample proves the design's rule file unmarshals into the typed set
// and survives a marshal and unmarshal unchanged.
func TestShapingSetExample(t *testing.T) {
	var set ShapingSet
	if err := json.Unmarshal([]byte(exampleSet), &set); err != nil {
		t.Fatalf("unmarshal example: %v", err)
	}

	want := ShapingSet{
		ID:             "lost-ack-30",
		ConnectionName: "^adr50-fast-lostack$",
		Rules: []ShapingRule{
			{
				ID: "drop-ack-30",
				Match: ShapingMatch{
					Direction: "from_server",
					Verb:      []string{"MSG"},
					Subject: &SubjectMatch{
						Grammar: "{prefix:rest}.{flow:int}.{gap}.{seq:int}.{op:int}.$FI",
						Where:   map[string]any{"seq": float64(30)},
					},
					Payload: &PayloadMatch{JSON: map[string]any{"type": "ack"}},
				},
				Action: ShapingAction{Kind: ShapingDrop},
				Limit:  1,
			},
			{
				ID:     "stall-pub-100",
				Match:  ShapingMatch{Direction: "to_server", Verb: []string{"PUB", "HPUB"}},
				Nth:    100,
				Action: ShapingAction{Kind: ShapingStall, Duration: "3s"},
			},
			{
				ID:     "slow-link",
				Match:  ShapingMatch{Direction: "to_server"},
				Action: ShapingAction{Kind: ShapingThrottle, Rate: "50/s", Burst: 10},
			},
		},
	}
	if !reflect.DeepEqual(set, want) {
		t.Errorf("example unmarshaled as %+v, want %+v", set, want)
	}

	out, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var again ShapingSet
	if err := json.Unmarshal(out, &again); err != nil {
		t.Fatalf("unmarshal round trip: %v", err)
	}
	if !reflect.DeepEqual(again, want) {
		t.Errorf("round trip gave %+v, want %+v", again, want)
	}
}
