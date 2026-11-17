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
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/synadia-io/orbit.go/ntf/backend"
)

// stepdownKind is a Kind whose actions need what their params say: the
// account of the streams they act on, the system account to wait for health,
// and the kill capability to kill the leader rather than step it down.
type stepdownKind struct{}

type stepdownParams struct {
	Streams    StreamSelector `json:"streams"`
	HealthWait bool           `json:"health_wait,omitempty"`
	Kill       bool           `json:"kill,omitempty"`
}

func (stepdownKind) Info() KindInfo {
	return KindInfo{Schema: json.RawMessage(`{"type":"object"}`)}
}

func (stepdownKind) New(params json.RawMessage) (Action, error) {
	var p stepdownParams
	err := DecodeParams(params, &p)
	if err != nil {
		return nil, err
	}
	err = p.Streams.Validate()
	if err != nil {
		return nil, err
	}

	return stepdownAction{params: p}, nil
}

type stepdownAction struct {
	params stepdownParams
}

func (a stepdownAction) Needs() Needs {
	needs := Needs{Accounts: []string{a.params.Streams.Account}}
	if a.params.HealthWait {
		needs.Accounts = append(needs.Accounts, SystemAccount)
	}
	if a.params.Kill {
		needs.Capabilities = append(needs.Capabilities, backend.CapabilityKill)
	}

	return needs
}

func (stepdownAction) Apply(context.Context, Target, StepReporter) (Effect, error) {
	return nil, nil
}

func TestActionNeedsFollowParams(t *testing.T) {
	cases := []struct {
		name         string
		params       string
		accounts     []string
		capabilities []backend.Capability
	}{
		{"stream account", `{"streams":{"account":"USERS1"}}`, []string{"USERS1"}, nil},
		{"health wait", `{"streams":{"account":"USERS2"},"health_wait":true}`, []string{"USERS2", SystemAccount}, nil},
		{"kill", `{"streams":{"account":"USERS1"},"kill":true}`, []string{"USERS1"}, []backend.Capability{backend.CapabilityKill}},
	}

	var kind Kind = stepdownKind{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			action, err := kind.New(json.RawMessage(tc.params))
			if err != nil {
				t.Fatal(err)
			}

			needs := action.Needs()
			if !slices.Equal(needs.Accounts, tc.accounts) {
				t.Errorf("accounts: got %v, want %v", needs.Accounts, tc.accounts)
			}
			if !slices.Equal(needs.Capabilities, tc.capabilities) {
				t.Errorf("capabilities: got %v, want %v", needs.Capabilities, tc.capabilities)
			}
			if needs.Toxics {
				t.Error("needs toxics")
			}
		})
	}

	_, err := kind.New(json.RawMessage(`{"streams":{}}`))
	if !errors.Is(err, ErrInvalidSelector) {
		t.Fatalf("params with no account: got %v, want ErrInvalidSelector", err)
	}
}
