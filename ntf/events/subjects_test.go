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
	"errors"
	"strings"
	"testing"
)

// TestSubjectEveryAction builds the subject of every type and action.
func TestSubjectEveryAction(t *testing.T) {
	b := Base{InstanceID: "INST"}
	cases := []struct {
		event Event
		want  string
	}{
		{&InstanceAction{Base: b, Action: InstanceCreated}, "ntf.events.tester.INST.instance.created"},
		{&InstanceAction{Base: b, Action: InstanceDestroyed}, "ntf.events.tester.INST.instance.destroyed"},
		{&NodeAction{Base: b, Action: NodeReady}, "ntf.events.tester.INST.node.ready"},
		{&NodeAction{Base: b, Action: NodeStopped}, "ntf.events.tester.INST.node.stopped"},
		{&NodeAction{Base: b, Action: NodeStarted}, "ntf.events.tester.INST.node.started"},
		{&NodeAction{Base: b, Action: NodeUpdated}, "ntf.events.tester.INST.node.updated"},
		{&NodeAction{Base: b, Action: NodeReloaded}, "ntf.events.tester.INST.node.reloaded"},
		{&NodeAction{Base: b, Action: NodeKilled}, "ntf.events.tester.INST.node.killed"},
		{&NodeAction{Base: b, Action: NodePaused}, "ntf.events.tester.INST.node.paused"},
		{&NodeAction{Base: b, Action: NodeResumed}, "ntf.events.tester.INST.node.resumed"},
		{&ShapingAction{Base: b, Action: ShapingApplied}, "ntf.events.tester.INST.shaping.applied"},
		{&ShapingAction{Base: b, Action: ShapingCleared}, "ntf.events.tester.INST.shaping.cleared"},
		{&ShapingFired{Base: b}, "ntf.events.tester.INST.shaping.fired"},
		{&CaptureStored{Base: b}, "ntf.events.tester.INST.capture.stored"},
	}

	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			got, err := Subject("tester", c.event)
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestSubjectRefusesBadTokens(t *testing.T) {
	cases := map[string]Event{
		"no instance":       &NodeAction{Action: NodeReady},
		"dotted instance":   &NodeAction{Base: Base{InstanceID: "a.b"}, Action: NodeReady},
		"wildcard instance": &NodeAction{Base: Base{InstanceID: "*"}, Action: NodeReady},
		"no action":         &NodeAction{Base: Base{InstanceID: "INST"}},
		"spaced action":     &NodeAction{Base: Base{InstanceID: "INST"}, Action: "went away"},
		"unknown event":     &UnknownEvent{Base: Base{InstanceID: "INST"}},
	}

	for name, e := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Subject("tester", e)
			if !errors.Is(err, ErrInvalidSubject) {
				t.Errorf("got %v, want ErrInvalidSubject", err)
			}
		})
	}

	_, err := Subject("Tester", &NodeAction{Base: Base{InstanceID: "INST"}, Action: NodeReady})
	if !errors.Is(err, ErrInvalidGroup) {
		t.Errorf("upper case group: got %v, want ErrInvalidGroup", err)
	}
}

func TestStreamSubjectsAndName(t *testing.T) {
	got := StreamSubject("tester")
	if got != "ntf.events.tester.>" {
		t.Errorf("StreamSubject is %q", got)
	}
	got = InstanceFilter("tester", "INST")
	if got != "ntf.events.tester.INST.>" {
		t.Errorf("InstanceFilter is %q", got)
	}
	got = StreamName("my-group_1")
	if got != "NTF_EVENTS_MY-GROUP_1" {
		t.Errorf("StreamName is %q", got)
	}
}

func TestValidateGroup(t *testing.T) {
	for _, group := range []string{"tester", "a", "team-1", "ci_run_2", strings.Repeat("a", 244)} {
		err := ValidateGroup(group)
		if err != nil {
			t.Errorf("%q refused: %v", group, err)
		}
	}

	bad := []string{"", "Tester", "TESTER", "a.b", "*", ">", "a*", "a>", "a b", " tester", "a\tb", "a/b", `a\b`, "caf\xc3\xa9", strings.Repeat("a", 245)}
	for _, group := range bad {
		err := ValidateGroup(group)
		if !errors.Is(err, ErrInvalidGroup) {
			t.Errorf("%q: got %v, want ErrInvalidGroup", group, err)
		}
	}
}
