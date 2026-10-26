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

package chaos

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// exampleYAML is the overview's example definition, with credentials.
const exampleYAML = `
duration: 30m
quiet:
  min: 30s
  max_fault: 2m
credentials:
  USERS1:
    user_password: {user: user1, password: password}
  $SYS:
    user_password: {user: system, password: secret}
workers:
  - name: kicks
    driver:
      interval: {every: 20s, jitter: 2s}
    action:
      client_disconnect: {mode: kick, clients: {fraction: 0.1}}
  - name: roll
    driver:
      once: {start_after: 5m}
    action:
      rolling_restart: {health_wait: true}
  - name: partition
    driver:
      steady: {open: 1, every: 2m}
    action:
      toxic: {type: cut, links: {nodes: {count: 1}}}
`

// exampleJSON is exampleYAML in JSON.
const exampleJSON = `{
  "duration": "30m",
  "quiet": {"min": "30s", "max_fault": "2m"},
  "credentials": {
    "USERS1": {"user_password": {"user": "user1", "password": "password"}},
    "$SYS": {"user_password": {"user": "system", "password": "secret"}}
  },
  "workers": [
    {"name": "kicks", "driver": {"interval": {"every": "20s", "jitter": "2s"}}, "action": {"client_disconnect": {"mode": "kick", "clients": {"fraction": 0.1}}}},
    {"name": "roll", "driver": {"once": {"start_after": "5m"}}, "action": {"rolling_restart": {"health_wait": true}}},
    {"name": "partition", "driver": {"steady": {"open": 1, "every": "2m"}}, "action": {"toxic": {"type": "cut", "links": {"nodes": {"count": 1}}}}}
  ]
}`

func checkExample(t *testing.T, def *Definition) {
	t.Helper()

	if time.Duration(def.Duration) != 30*time.Minute {
		t.Errorf("duration: got %v", time.Duration(def.Duration))
	}
	if time.Duration(def.Quiet.Min) != 30*time.Second || time.Duration(def.Quiet.MaxFault) != 2*time.Minute {
		t.Errorf("quiet: got %+v", def.Quiet)
	}
	if def.Credentials[SystemAccount].UserPassword.User != "system" {
		t.Errorf("system credentials: got %+v", def.Credentials[SystemAccount])
	}
	if len(def.Workers) != 3 {
		t.Fatalf("workers: got %d", len(def.Workers))
	}

	w := def.Workers[0]
	if w.Name != "kicks" || w.Driver.Name != "interval" || w.Action.Name != "client_disconnect" {
		t.Errorf("worker 0: got %s %s %s", w.Name, w.Driver.Name, w.Action.Name)
	}

	var interval struct {
		Every  Duration `json:"every"`
		Jitter Duration `json:"jitter"`
	}
	err := DecodeParams(w.Driver.Params, &interval)
	if err != nil {
		t.Fatalf("decode interval params: %v", err)
	}
	if time.Duration(interval.Every) != 20*time.Second || time.Duration(interval.Jitter) != 2*time.Second {
		t.Errorf("interval params: got %+v", interval)
	}

	var toxic struct {
		Type  string       `json:"type"`
		Links LinkSelector `json:"links"`
	}
	err = DecodeParams(def.Workers[2].Action.Params, &toxic)
	if err != nil {
		t.Fatalf("decode toxic params: %v", err)
	}
	if toxic.Type != "cut" || toxic.Links.Nodes == nil || toxic.Links.Nodes.Count != 1 {
		t.Errorf("toxic params: got %+v", toxic)
	}
}

func TestParseDefinitionKeyedForm(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		def, err := ParseDefinition([]byte(exampleJSON))
		if err != nil {
			t.Fatal(err)
		}
		checkExample(t, def)
	})

	t.Run("yaml", func(t *testing.T) {
		def, err := ParseDefinitionYAML([]byte(exampleYAML))
		if err != nil {
			t.Fatal(err)
		}
		checkExample(t, def)
	})

	t.Run("round trip", func(t *testing.T) {
		def, err := ParseDefinitionYAML([]byte(exampleYAML))
		if err != nil {
			t.Fatal(err)
		}
		j, err := json.Marshal(def)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(j), `"driver":{"interval":{"every":"20s","jitter":"2s"}}`) {
			t.Errorf("driver is not in the keyed form: %s", j)
		}
		again, err := ParseDefinition(j)
		if err != nil {
			t.Fatalf("parse marshaled definition: %v", err)
		}
		checkExample(t, again)
	})
}

func TestParseDefinitionDefaults(t *testing.T) {
	def, err := ParseDefinitionYAML([]byte(`
duration: 10m
workers:
  - name: roll
    driver:
      once:
    action:
      rolling_restart: {}
`))
	if err != nil {
		t.Fatal(err)
	}

	if time.Duration(def.Quiet.Min) != DefaultQuietMin {
		t.Errorf("quiet.min: got %v, want %v", time.Duration(def.Quiet.Min), DefaultQuietMin)
	}
	if time.Duration(def.Quiet.MaxFault) != DefaultQuietMaxFault {
		t.Errorf("quiet.max_fault: got %v, want %v", time.Duration(def.Quiet.MaxFault), DefaultQuietMaxFault)
	}
	if def.Workers[0].Driver.Name != "once" || string(def.Workers[0].Driver.Params) != "{}" {
		t.Errorf("a driver with null params: got %q %s", def.Workers[0].Driver.Name, def.Workers[0].Driver.Params)
	}
}

func TestParseDefinitionRefusesUnknownFields(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		json string
	}{
		{
			"top level",
			"duration: 1m\nseed: 1\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]",
			`{"duration":"1m","seed":1,"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		},
		{
			"quiet",
			"duration: 1m\nquiet: {minimum: 1s}\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]",
			`{"duration":"1m","quiet":{"minimum":"1s"},"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		},
		{
			"worker",
			"duration: 1m\nworkers: [{name: a, kind: x, driver: {once: {}}, action: {x: {}}}]",
			`{"duration":"1m","workers":[{"name":"a","kind":"x","driver":{"once":{}},"action":{"x":{}}}]}`,
		},
		{
			"credential type",
			"duration: 1m\ncredentials: {A: {token: {token: x}}}\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]",
			`{"duration":"1m","credentials":{"A":{"token":{"token":"x"}}},"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		},
		{
			"user_password",
			"duration: 1m\ncredentials: {A: {user_password: {user: u, password: p, nkey: k}}}\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]",
			`{"duration":"1m","credentials":{"A":{"user_password":{"user":"u","password":"p","nkey":"k"}}},"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name+" json", func(t *testing.T) {
			_, err := ParseDefinition([]byte(tc.json))
			if !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("got %v, want ErrInvalidDefinition", err)
			}
			if !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("error %q does not name an unknown field", err)
			}
		})
		t.Run(tc.name+" yaml", func(t *testing.T) {
			_, err := ParseDefinitionYAML([]byte(tc.yaml))
			if !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("got %v, want ErrInvalidDefinition", err)
			}
			if !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("error %q does not name an unknown field", err)
			}
		})
	}
}

func TestParseDefinitionInvalid(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"no duration", "workers: [{name: a, driver: {once: {}}, action: {x: {}}}]", "duration must be above zero"},
		{"negative duration", "duration: -1m\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]", "duration must be above zero"},
		{"numeric duration", "duration: 60\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]", "duration must be a string"},
		{"bad duration", "duration: 1 minute\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]", "unknown unit"},
		{"negative quiet", "duration: 1m\nquiet: {min: -1s}\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]", "quiet.min"},
		{"no workers", "duration: 1m", "at least one worker"},
		{"no worker name", "duration: 1m\nworkers: [{driver: {once: {}}, action: {x: {}}}]", "has no name"},
		{"long worker name", "duration: 1m\nworkers: [{name: " + strings.Repeat("w", MaxWorkerNameLength+1) + ", driver: {once: {}}, action: {x: {}}}]", "longer than 32"},
		{"worker name with a dot", "duration: 1m\nworkers: [{name: a.b, driver: {once: {}}, action: {x: {}}}]", "only letters"},
		{"duplicate worker", "duration: 1m\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}, {name: a, driver: {once: {}}, action: {x: {}}}]", "used twice"},
		{"no driver", "duration: 1m\nworkers: [{name: a, action: {x: {}}}]", "has no driver"},
		{"no action", "duration: 1m\nworkers: [{name: a, driver: {once: {}}}]", "has no action"},
		{"two keys", "duration: 1m\nworkers: [{name: a, driver: {once: {}, interval: {}}, action: {x: {}}}]", "got 2 keys"},
		{"no keys", "duration: 1m\nworkers: [{name: a, driver: {}, action: {x: {}}}]", "got 0 keys"},
		{"bare name", "duration: 1m\nworkers: [{name: a, driver: once, action: {x: {}}}]", "keyed by one name"},
		{"scalar params", "duration: 1m\nworkers: [{name: a, driver: {once: 5m}, action: {x: {}}}]", "must be an object"},
		{"credential without type", "duration: 1m\ncredentials: {A: {}}\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]", "no credential type"},
		{"credential without user", "duration: 1m\ncredentials: {A: {user_password: {password: p}}}\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]", "needs a user"},
		{"duplicate key", "duration: 1m\nduration: 2m\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]", `mapping key "duration" already defined`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDefinitionYAML([]byte(tc.yaml))
			if !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("got %v, want ErrInvalidDefinition", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}

	_, err := ParseDefinition([]byte(exampleJSON + ` {}`))
	if !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("trailing data: got %v, want ErrInvalidDefinition", err)
	}
}

func TestParseDefinitionYAMLDocuments(t *testing.T) {
	_, err := ParseDefinitionYAML([]byte("---\n" + exampleYAML))
	if err != nil {
		t.Fatalf("one document after a marker: %v", err)
	}

	second := "duration: 1m\nworkers: [{name: b, driver: {once: {}}, action: {x: {}}}]\n"
	for name, data := range map[string]string{
		"two documents":         exampleYAML + "---\n" + second,
		"marked documents":      "---\n" + exampleYAML + "---\n" + second,
		"ended documents":       exampleYAML + "...\n---\n" + second,
		"empty second document": exampleYAML + "---\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseDefinitionYAML([]byte(data))
			if !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("got %v, want ErrInvalidDefinition", err)
			}
			if !strings.Contains(err.Error(), "YAML documents") {
				t.Fatalf("error %q does not name the documents", err)
			}
		})
	}
}

// TestParseDefinitionYAMLHidesSource checks a YAML error names its line and
// column without quoting the source, so a password near it is not shown.
func TestParseDefinitionYAMLHidesSource(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			"malformed line after the password",
			"duration: 1m\ncredentials:\n  A:\n    user_password:\n      user: u\n      password: hunter2\n     oops: [\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]\n",
			"[7:",
		},
		{
			"duplicate password key",
			"duration: 1m\ncredentials:\n  A:\n    user_password:\n      user: u\n      password: hunter2\n      password: hunter2\nworkers: [{name: a, driver: {once: {}}, action: {x: {}}}]\n",
			`[7:7] mapping key "password" already defined at [6:7]`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDefinitionYAML([]byte(tc.yaml))
			if !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("got %v, want ErrInvalidDefinition", err)
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Fatalf("error shows the password:\n%v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestParseDefinitionRefusesDuplicateKeys(t *testing.T) {
	cases := map[string]string{
		"top level":       `{"duration":"1m","duration":"2m","workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		"top level apart": `{"duration":"1m","workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}],"duration":"2m"}`,
		"quiet":           `{"duration":"1m","quiet":{"min":"1s","min":"2s"},"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		"credentials":     `{"duration":"1m","credentials":{"A":{"user_password":{"user":"u"}},"A":{"user_password":{"user":"v"}}},"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		"user":            `{"duration":"1m","credentials":{"A":{"user_password":{"user":"u","user":"v"}}},"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		"worker":          `{"duration":"1m","workers":[{"name":"a","name":"b","driver":{"once":{}},"action":{"x":{}}}]}`,
		"keyed form":      `{"duration":"1m","workers":[{"name":"a","driver":{"once":{"a":1},"once":{"b":2}},"action":{"x":{}}}]}`,
		"params":          `{"duration":"1m","workers":[{"name":"a","driver":{"once":{}},"action":{"x":{"links":{"nodes":{"count":1,"count":2}}}}}]}`,
		"in an array":     `{"duration":"1m","workers":[{"name":"a","driver":{"once":{}},"action":{"x":{"list":[{},{"k":1,"k":2}]}}}]}`,
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseDefinition([]byte(data))
			if !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("got %v, want ErrInvalidDefinition", err)
			}
			if !strings.Contains(err.Error(), "more than once") {
				t.Fatalf("error %q does not name a duplicate key", err)
			}
		})
	}

	_, err := ParseDefinition([]byte(`{"duration":"1m","workers":[{"name":"a","driver":{"once":{"a":{"k":1},"b":{"k":2}}},"action":{"x":{"big":1e400,"k":[{"k":1},{"k":2}]}}}]}`))
	if err != nil {
		t.Fatalf("the same key in sibling objects: %v", err)
	}
}

func TestDecodeParams(t *testing.T) {
	var p struct {
		Mode string `json:"mode"`
	}

	err := DecodeParams(json.RawMessage(`{"mode":"kick"}`), &p)
	if err != nil || p.Mode != "kick" {
		t.Fatalf("got %+v %v", p, err)
	}

	err = DecodeParams(nil, &p)
	if err != nil {
		t.Fatalf("empty params: %v", err)
	}

	err = DecodeParams(json.RawMessage(`{"mode":"kick","count":2}`), &p)
	if !errors.Is(err, ErrInvalidParams) {
		t.Fatalf("unknown field: got %v, want ErrInvalidParams", err)
	}

	err = DecodeParams(json.RawMessage(`{"Mode":"kick"}`), &p)
	if !errors.Is(err, ErrInvalidParams) || !strings.Contains(err.Error(), "case sensitive") {
		t.Fatalf("field in another case: got %v, want ErrInvalidParams", err)
	}

	var links struct {
		Links LinkSelector `json:"links"`
	}
	err = DecodeParams(json.RawMessage(`{"links":{"nodes":{"Count":1}}}`), &links)
	if !errors.Is(err, ErrInvalidParams) || !strings.Contains(err.Error(), "case sensitive") {
		t.Fatalf("nested field in another case: got %v, want ErrInvalidParams", err)
	}

	err = DecodeParams(json.RawMessage(`{"links":{"kind":"route","nodes":{"names":["n1"],"count":1}}}`), &links)
	if err != nil {
		t.Fatalf("exact field names: %v", err)
	}
}

func TestParseDefinitionRefusesFieldCase(t *testing.T) {
	cases := map[string]string{
		"top level":       `{"Duration":"1m","workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		"twice by case":   `{"duration":"1m","Duration":"2m","workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		"quiet":           `{"duration":"1m","quiet":{"MAX_FAULT":"1m"},"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		"credential type": `{"duration":"1m","credentials":{"A":{"User_Password":{"user":"u"}}},"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		"user":            `{"duration":"1m","credentials":{"A":{"user_password":{"User":"u"}}},"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]}`,
		"worker":          `{"duration":"1m","workers":[{"name":"a","Driver":{"once":{}},"action":{"x":{}}}]}`,
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseDefinition([]byte(data))
			if !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("got %v, want ErrInvalidDefinition", err)
			}
			if !strings.Contains(err.Error(), "case sensitive") {
				t.Fatalf("error %q does not name the case", err)
			}
		})
	}

	def, err := ParseDefinition([]byte(`{"duration":"1m","workers":[{"name":"a","driver":{"Once":{"Every":"1s"}},"action":{"x":{}}}]}`))
	if err != nil {
		t.Fatalf("keyed names and params in any case: %v", err)
	}
	if def.Workers[0].Driver.Name != "Once" {
		t.Fatalf("driver name: got %q", def.Workers[0].Driver.Name)
	}
}
