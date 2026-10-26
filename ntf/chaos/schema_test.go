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
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func compileDefinitionSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()

	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(DefinitionSchema()))
	if err != nil {
		t.Fatalf("definition schema is not JSON: %v", err)
	}

	c := jsonschema.NewCompiler()
	c.AssertFormat()
	err = c.AddResource("definition.json", doc)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := c.Compile("definition.json")
	if err != nil {
		t.Fatalf("could not compile the definition schema: %v", err)
	}

	return sch
}

func validateAgainst(sch *jsonschema.Schema, data []byte) error {
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return err
	}

	return sch.Validate(inst)
}

func TestDefinitionSchema(t *testing.T) {
	sch := compileDefinitionSchema(t)

	t.Run("example", func(t *testing.T) {
		err := validateAgainst(sch, []byte(exampleJSON))
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("marshaled definition", func(t *testing.T) {
		def := Definition{
			Duration:    Duration(90 * time.Minute),
			Credentials: map[string]Credential{"USERS1": {UserPassword: &UserPassword{User: "user1", Password: "password"}}},
			Workers: []Worker{{
				Name:   "w_1",
				Driver: Spec{Name: "interval", Params: json.RawMessage(`{"every":"1m30s"}`)},
				Action: Spec{Name: "node_reload"},
			}},
		}
		def.SetDefaults()

		j, err := json.Marshal(def)
		if err != nil {
			t.Fatal(err)
		}
		err = validateAgainst(sch, j)
		if err != nil {
			t.Fatalf("%s: %v", j, err)
		}
	})

	// The schema and ParseDefinition accept each valid case and refuse each
	// other one alike. ParseDefinition also checks what the schema leaves
	// out, that durations are in range, such as a run duration above zero, and
	// that worker names are unique, so no case here is refused for those
	// alone. Duplicate keys are refused as ParseDefinition reads the JSON,
	// which a schema cannot see, and are tested with ParseDefinition.
	const worker = `"workers":[{"name":"a","driver":{"once":{}},"action":{"x":{}}}]`
	cases := []struct {
		name  string
		data  string
		valid bool
	}{
		{"zero durations", `{"duration":"1m","quiet":{"min":"0","max_fault":"0s"},` + worker + `}`, true},
		{"signed durations", `{"duration":"+1m","quiet":{"min":"-0s","max_fault":"+0"},` + worker + `}`, true},
		{"micro units", `{"duration":"1us","quiet":{"min":"1\u00b5s","max_fault":"1\u03bcs"},` + worker + `}`, true},
		{"compound duration", `{"duration":"1.5h0.5m.25s10ms7ns","quiet":{"min":"1.s"},` + worker + `}`, true},
		{"empty quiet", `{"duration":"1m","quiet":{},` + worker + `}`, true},
		{"null quiet", `{"duration":"1m","quiet":null,` + worker + `}`, true},
		{"empty credentials", `{"duration":"1m","credentials":{},` + worker + `}`, true},
		{"null credentials", `{"duration":"1m","credentials":null,` + worker + `}`, true},
		{"no password", `{"duration":"1m","credentials":{"A":{"user_password":{"user":"u"}}},` + worker + `}`, true},
		{"empty password", `{"duration":"1m","credentials":{"A":{"user_password":{"user":"u","password":""}}},` + worker + `}`, true},
		{"null password", `{"duration":"1m","credentials":{"A":{"user_password":{"user":"u","password":null}}},` + worker + `}`, true},
		{"null params", `{"duration":"1m","workers":[{"name":"a","driver":{"once":null},"action":{"x":{}}}]}`, true},

		{"unknown field", `{"duration":"1m","seed":1,` + worker + `}`, false},
		{"no workers", `{"duration":"1m","workers":[]}`, false},
		{"null workers", `{"duration":"1m","workers":null}`, false},
		{"null worker", `{"duration":"1m","workers":[null]}`, false},
		{"no duration", `{` + worker + `}`, false},
		{"numeric duration", `{"duration":60,` + worker + `}`, false},
		{"null duration", `{"duration":null,` + worker + `}`, false},
		{"bad duration", `{"duration":"1 minute",` + worker + `}`, false},
		{"empty duration", `{"duration":"",` + worker + `}`, false},
		{"number without unit", `{"duration":"1",` + worker + `}`, false},
		{"double zero", `{"duration":"00",` + worker + `}`, false},
		{"unit without number", `{"duration":"s",` + worker + `}`, false},
		{"dot without digits", `{"duration":".s",` + worker + `}`, false},
		{"sign inside", `{"duration":"1h-1m",` + worker + `}`, false},
		{"two signs", `{"duration":"+-1m",` + worker + `}`, false},
		{"unknown unit", `{"duration":"1d",` + worker + `}`, false},
		{"null quiet min", `{"duration":"1m","quiet":{"min":null},` + worker + `}`, false},
		{"unknown quiet field", `{"duration":"1m","quiet":{"minimum":"1s"},` + worker + `}`, false},
		{"two keys", `{"duration":"1m","workers":[{"name":"a","driver":{"once":{},"interval":{}},"action":{"x":{}}}]}`, false},
		{"no keys", `{"duration":"1m","workers":[{"name":"a","driver":{},"action":{"x":{}}}]}`, false},
		{"empty key", `{"duration":"1m","workers":[{"name":"a","driver":{"":{}},"action":{"x":{}}}]}`, false},
		{"null driver", `{"duration":"1m","workers":[{"name":"a","driver":null,"action":{"x":{}}}]}`, false},
		{"scalar params", `{"duration":"1m","workers":[{"name":"a","driver":{"once":"5m"},"action":{"x":{}}}]}`, false},
		{"array params", `{"duration":"1m","workers":[{"name":"a","driver":{"once":[]},"action":{"x":{}}}]}`, false},
		{"long worker name", `{"duration":"1m","workers":[{"name":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","driver":{"once":{}},"action":{"x":{}}}]}`, false},
		{"worker name with dot", `{"duration":"1m","workers":[{"name":"a.b","driver":{"once":{}},"action":{"x":{}}}]}`, false},
		{"empty worker name", `{"duration":"1m","workers":[{"name":"","driver":{"once":{}},"action":{"x":{}}}]}`, false},
		{"unknown credential", `{"duration":"1m","credentials":{"A":{"token":{"token":"x"}}},` + worker + `}`, false},
		{"credential no type", `{"duration":"1m","credentials":{"A":{}},` + worker + `}`, false},
		{"null credential", `{"duration":"1m","credentials":{"A":null},` + worker + `}`, false},
		{"null user_password", `{"duration":"1m","credentials":{"A":{"user_password":null}},` + worker + `}`, false},
		{"credential no user", `{"duration":"1m","credentials":{"A":{"user_password":{"password":"p"}}},` + worker + `}`, false},
		{"credential empty user", `{"duration":"1m","credentials":{"A":{"user_password":{"user":"","password":"p"}}},` + worker + `}`, false},
		{"credential empty account", `{"duration":"1m","credentials":{"":{"user_password":{"user":"u"}}},` + worker + `}`, false},
		{"field in another case", `{"Duration":"1m",` + worker + `}`, false},
		{"field twice by case", `{"duration":"1m","Duration":"2m",` + worker + `}`, false},
		{"quiet field in another case", `{"duration":"1m","quiet":{"MAX_FAULT":"1m"},` + worker + `}`, false},
		{"credential type in another case", `{"duration":"1m","credentials":{"A":{"User_Password":{"user":"u"}}},` + worker + `}`, false},
		{"user in another case", `{"duration":"1m","credentials":{"A":{"user_password":{"User":"u"}}},` + worker + `}`, false},
		{"worker field in another case", `{"duration":"1m","workers":[{"Name":"a","driver":{"once":{}},"action":{"x":{}}}]}`, false},
		{"unknown worker field", `{"duration":"1m","workers":[{"name":"a","kind":"x","driver":{"once":{}},"action":{"x":{}}}]}`, false},
		{"worker without action", `{"duration":"1m","workers":[{"name":"a","driver":{"once":{}}}]}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schemaErr := validateAgainst(sch, []byte(tc.data))
			if tc.valid && schemaErr != nil {
				t.Errorf("schema refused %s: %v", tc.data, schemaErr)
			}
			if !tc.valid && schemaErr == nil {
				t.Errorf("schema accepted %s", tc.data)
			}

			_, parseErr := ParseDefinition([]byte(tc.data))
			if tc.valid && parseErr != nil {
				t.Errorf("ParseDefinition refused %s: %v", tc.data, parseErr)
			}
			if !tc.valid && parseErr == nil {
				t.Errorf("ParseDefinition accepted %s", tc.data)
			}
		})
	}
}
