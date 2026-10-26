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
)

func TestCredentialValidate(t *testing.T) {
	err := Credential{UserPassword: &UserPassword{User: "u", Password: "p"}}.Validate()
	if err != nil {
		t.Fatal(err)
	}

	for _, bad := range []Credential{{}, {UserPassword: &UserPassword{Password: "p"}}} {
		err = bad.Validate()
		if !errors.Is(err, ErrInvalidCredential) {
			t.Errorf("%+v: got %v, want ErrInvalidCredential", bad, err)
		}
	}
}

func TestDefinitionRedacted(t *testing.T) {
	def, err := ParseDefinitionYAML([]byte(exampleYAML))
	if err != nil {
		t.Fatal(err)
	}

	redacted := def.Redacted()

	j, err := json.Marshal(redacted)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{`"password":"password"`, `"secret"`} {
		if strings.Contains(string(j), secret) {
			t.Errorf("redacted definition holds %s: %s", secret, j)
		}
	}
	if !strings.Contains(string(j), `"user_password":{"user":"system","password":"[redacted]"}`) {
		t.Errorf("redacted definition does not keep the user and mark the password: %s", j)
	}
	if len(redacted.Workers) != len(def.Workers) || redacted.Duration != def.Duration {
		t.Errorf("redaction changed more than the credentials: %s", j)
	}

	if def.Credentials[SystemAccount].UserPassword.Password != "secret" {
		t.Error("redaction changed the original definition")
	}

	redacted.Workers[0].Name = "changed"
	if def.Workers[0].Name != "kicks" {
		t.Error("the redacted copy shares its workers with the original")
	}

	empty := Definition{}.Redacted()
	if empty.Credentials != nil {
		t.Errorf("redacting no credentials made %v", empty.Credentials)
	}
}
