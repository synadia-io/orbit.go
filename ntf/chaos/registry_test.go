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
	"strings"
	"testing"
)

// fakeKind is a Kind whose actions do nothing.
type fakeKind struct{}

func (fakeKind) Info() KindInfo {
	return KindInfo{Schema: json.RawMessage(`{"type":"object"}`)}
}

func (fakeKind) New(json.RawMessage) (Action, error) {
	return fakeAction{}, nil
}

type fakeAction struct{}

func (fakeAction) Needs() Needs {
	return Needs{}
}

func (fakeAction) Apply(context.Context, Target, StepReporter) (Effect, error) {
	return nil, nil
}

// fakeDriver is a Driver whose schedules are finished at once.
type fakeDriver struct{}

func (fakeDriver) Info() DriverInfo {
	return DriverInfo{Schema: json.RawMessage(`{"type":"object"}`)}
}

func (fakeDriver) New(json.RawMessage, Clock) (Schedule, error) {
	return fakeSchedule{}, nil
}

type fakeSchedule struct{}

func (fakeSchedule) Next(context.Context, int) (Act, error) {
	return Act{}, ErrDriverDone
}

func TestKindRegistry(t *testing.T) {
	r := NewKindRegistry()

	if len(r.Names()) != 0 {
		t.Fatalf("new registry has names %v", r.Names())
	}

	_, err := r.Lookup("toxic")
	if !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("lookup in an empty registry: got %v, want ErrUnknownKind", err)
	}
	if !strings.Contains(err.Error(), "registered: none") {
		t.Fatalf("empty registry error %q does not say none are registered", err)
	}

	for _, name := range []string{"toxic", "node_reload", "client_disconnect"} {
		err = r.Register(name, fakeKind{})
		if err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
	}

	want := []string{"client_disconnect", "node_reload", "toxic"}
	if !slices.Equal(r.Names(), want) {
		t.Fatalf("names: got %v, want %v", r.Names(), want)
	}

	k, err := r.Lookup("toxic")
	if err != nil {
		t.Fatalf("lookup toxic: %v", err)
	}
	if k == nil {
		t.Fatal("lookup toxic returned nil")
	}

	err = r.Register("", fakeKind{})
	if !errors.Is(err, ErrKindName) {
		t.Fatalf("empty name: got %v, want ErrKindName", err)
	}

	err = r.Register("toxic", fakeKind{})
	if !errors.Is(err, ErrKindName) {
		t.Fatalf("duplicate name: got %v, want ErrKindName", err)
	}

	err = r.Register("nil", nil)
	if !errors.Is(err, ErrKindName) {
		t.Fatalf("nil kind: got %v, want ErrKindName", err)
	}

	_, err = r.Lookup("kill")
	if !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("unknown name: got %v, want ErrUnknownKind", err)
	}
	if !strings.Contains(err.Error(), `"kill", registered: client_disconnect, node_reload, toxic`) {
		t.Fatalf("unknown name error %q does not list the registered names", err)
	}

	_, err = r.Lookup("")
	if !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("lookup of an empty name: got %v, want ErrUnknownKind", err)
	}
}

func TestDriverRegistry(t *testing.T) {
	r := NewDriverRegistry()

	err := r.Register("interval", fakeDriver{})
	if err != nil {
		t.Fatalf("register interval: %v", err)
	}

	d, err := r.Lookup("interval")
	if err != nil {
		t.Fatalf("lookup interval: %v", err)
	}
	if d == nil {
		t.Fatal("lookup interval returned nil")
	}

	err = r.Register("", fakeDriver{})
	if !errors.Is(err, ErrDriverName) {
		t.Fatalf("empty name: got %v, want ErrDriverName", err)
	}

	err = r.Register("interval", fakeDriver{})
	if !errors.Is(err, ErrDriverName) {
		t.Fatalf("duplicate name: got %v, want ErrDriverName", err)
	}

	err = r.Register("nil", nil)
	if !errors.Is(err, ErrDriverName) {
		t.Fatalf("nil driver: got %v, want ErrDriverName", err)
	}

	_, err = r.Lookup("on_event")
	if !errors.Is(err, ErrUnknownDriver) {
		t.Fatalf("unknown name: got %v, want ErrUnknownDriver", err)
	}
	if errors.Is(err, ErrUnknownKind) {
		t.Fatalf("driver registry error %v wraps ErrUnknownKind", err)
	}
	if !strings.Contains(err.Error(), `"on_event", registered: interval`) {
		t.Fatalf("unknown name error %q does not list the registered names", err)
	}
}

func TestRegistriesAreIndependent(t *testing.T) {
	a := NewKindRegistry()
	b := NewKindRegistry()

	err := a.Register("toxic", fakeKind{})
	if err != nil {
		t.Fatalf("register in a: %v", err)
	}

	_, err = b.Lookup("toxic")
	if !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("a kind registered in one registry is found in another: %v", err)
	}

	err = b.Register("toxic", fakeKind{})
	if err != nil {
		t.Fatalf("the same name in a second registry: %v", err)
	}
}
