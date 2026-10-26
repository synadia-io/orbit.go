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

import "fmt"

// RedactedSecret replaces every secret in a redacted credential.
const RedactedSecret = "[redacted]"

// Credential is how the engine authenticates to one account, in the keyed
// form: exactly one field is set, and its JSON key names the credential's
// type. Only user_password exists; other types are added as fields beside it,
// so definitions written today keep decoding.
type Credential struct {
	// UserPassword is a user and password.
	UserPassword *UserPassword `json:"user_password,omitempty"`
}

// UserPassword is a user and password credential.
type UserPassword struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// Validate checks that exactly one type is set and that it is complete.
func (c Credential) Validate() error {
	if c.UserPassword == nil {
		return fmt.Errorf("%w: no credential type set", ErrInvalidCredential)
	}
	if c.UserPassword.User == "" {
		return fmt.Errorf("%w: user_password needs a user", ErrInvalidCredential)
	}

	return nil
}

// Redacted returns a copy of c with every secret replaced by RedactedSecret,
// to show wherever c is shown. c is not changed.
func (c Credential) Redacted() Credential {
	var out Credential
	if c.UserPassword != nil {
		out.UserPassword = &UserPassword{User: c.UserPassword.User, Password: RedactedSecret}
	}

	return out
}
