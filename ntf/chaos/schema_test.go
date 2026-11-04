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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/synadia-io/orbit.go/ntf/schema/validate"
)

// schemaDraft is the $schema of every schema under schemas.
const schemaDraft = "https://json-schema.org/draft/2020-12/schema"

// retiredKeywords are the keywords of older drafts that 2020-12 replaced.
var retiredKeywords = []string{"definitions", "dependencies", "additionalItems", "$recursiveAnchor", "$recursiveRef"}

// namingKeywords hold subschemas keyed by a name of the schema's own, which
// may be spelled like a keyword.
var namingKeywords = []string{"$defs", "dependentSchemas", "patternProperties", "properties"}

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

// TestDefinitionSchemaRefusedKey proves a key the schema refuses is reported at
// the map that holds it, in a definition with siblings around that map.
func TestDefinitionSchemaRefusedKey(t *testing.T) {
	sch := compileDefinitionSchema(t)

	cases := []struct {
		name string
		body string
		want string
	}{
		{"driver", `{"duration":"1m","workers":[{"name":"a","driver":{"":{}},"action":{"x":{}}}]}`, "Invalid request: workers.0.driver: invalid propertyName ''"},
		{"action", `{"duration":"1m","workers":[{"name":"a","driver":{"x":{}},"action":{"":{}}}]}`, "Invalid request: workers.0.action: invalid propertyName ''"},
		{"credentials", `{"duration":"1m","credentials":{"":{}},"workers":[{"name":"a","driver":{"x":{}},"action":{"x":{}}}]}`, "Invalid request: credentials: invalid propertyName ''"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for range 200 {
				got := validate.Description(sch, []byte(tc.body))
				if got != tc.want {
					t.Fatalf("description = %q, want %q", got, tc.want)
				}
			}
		})
	}
}

// draftProblems lists what keeps a schema off 2020-12: a $schema naming anything
// else, a keyword of an older draft or a $ref that resolves to nothing.
// Compiling alone misses these, since the compiler still resolves a $ref into
// definitions under 2020-12 and follows only the $refs the root reaches.
func draftProblems(t *testing.T, data []byte) []string {
	t.Helper()

	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("schema is not JSON: %v", err)
	}
	root, ok := doc.(map[string]any)
	if !ok {
		t.Fatalf("schema is a %T, want an object", doc)
	}

	c := jsonschema.NewCompiler()
	err = c.AddResource("schema.json", doc)
	if err != nil {
		t.Fatal(err)
	}

	var problems []string
	if root["$schema"] != schemaDraft {
		problems = append(problems, fmt.Sprintf("$schema is %v, want %s", root["$schema"], schemaDraft))
	}
	_, err = c.Compile("schema.json")
	if err != nil {
		problems = append(problems, fmt.Sprintf("does not compile: %v", err))
	}

	var walk func(node any, at string)
	walk = func(node any, at string) {
		switch n := node.(type) {
		case []any:
			for i, v := range n {
				walk(v, fmt.Sprintf("%s/%d", at, i))
			}
		case map[string]any:
			for key, v := range n {
				if slices.Contains(retiredKeywords, key) {
					problems = append(problems, fmt.Sprintf("%s holds %s", at, key))
				}
				if ref, ok := v.(string); ok && key == "$ref" {
					_, err := c.Compile("schema.json" + ref)
					if err != nil {
						problems = append(problems, fmt.Sprintf("%s: $ref %s does not resolve: %v", at, ref, err))
					}
				}

				subs, ok := v.(map[string]any)
				if ok && slices.Contains(namingKeywords, key) {
					for name, sub := range subs {
						walk(sub, at+"/"+key+"/"+name)
					}
					continue
				}
				walk(v, at+"/"+key)
			}
		}
	}
	walk(doc, "#")

	return problems
}

func TestSchemasDraft(t *testing.T) {
	var found int
	err := fs.WalkDir(os.DirFS("schemas"), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		found++

		data, err := os.ReadFile(filepath.Join("schemas", path))
		if err != nil {
			return err
		}
		for _, problem := range draftProblems(t, data) {
			t.Errorf("%s: %s", path, problem)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found == 0 {
		t.Fatal("found no schemas")
	}

	// Each of these has to be caught for the walk above to mean anything.
	const draft = `"$schema":"` + schemaDraft + `"`
	cases := []struct {
		name   string
		schema string
		valid  bool
	}{
		{"current", `{` + draft + `,"$defs":{"a":{}},"properties":{"b":{"$ref":"#/$defs/a"}}}`, true},
		{"properties named as keywords", `{` + draft + `,"properties":{"definitions":{},"dependencies":{}}}`, true},

		{"draft-07", `{"$schema":"http://json-schema.org/draft-07/schema#"}`, false},
		{"no $schema", `{}`, false},
		{"definitions", `{` + draft + `,"definitions":{"a":{}}}`, false},
		{"nested definitions", `{` + draft + `,"properties":{"a":{"definitions":{}}}}`, false},
		{"$ref left on definitions", `{` + draft + `,"$defs":{"a":{}},"properties":{"b":{"$ref":"#/definitions/a"}}}`, false},
		{"$ref nothing reaches", `{` + draft + `,"$defs":{"a":{"$ref":"#/$defs/b"}}}`, false},
		{"dependencies", `{` + draft + `,"dependencies":{"a":["b"]}}`, false},
		{"tuple items", `{` + draft + `,"items":[{}]}`, false},
		{"additionalItems", `{` + draft + `,"additionalItems":false}`, false},
		{"$recursiveRef", `{` + draft + `,"$recursiveRef":"#"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			problems := draftProblems(t, []byte(tc.schema))
			if tc.valid && len(problems) > 0 {
				t.Errorf("refused %s: %v", tc.schema, problems)
			}
			if !tc.valid && len(problems) == 0 {
				t.Errorf("accepted %s", tc.schema)
			}
		})
	}
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
