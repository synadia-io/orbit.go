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
	"fmt"
	"strings"
)

// subjectPrefix is the first two tokens of every event subject.
const subjectPrefix = "ntf.events"

// streamPrefix starts the name of every events stream.
const streamPrefix = "NTF_EVENTS_"

// maxStreamName is the longest stream name JetStream accepts.
const maxStreamName = 255

// ValidateGroup checks that group is a single lowercase subject token valid in
// a stream name, made of a-z, 0-9, - and _ only. Two valid groups therefore
// never share a stream name or overlap in subjects.
func ValidateGroup(group string) error {
	if group == "" {
		return fmt.Errorf("%w: empty", ErrInvalidGroup)
	}
	if len(streamPrefix)+len(group) > maxStreamName {
		return fmt.Errorf("%w: longer than %d characters", ErrInvalidGroup, maxStreamName-len(streamPrefix))
	}

	for _, r := range group {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return fmt.Errorf("%w: %q", ErrInvalidGroup, group)
		}
	}
	return nil
}

// validToken reports whether s is a single subject token with no wildcard.
func validToken(s string) bool {
	if s == "" {
		return false
	}
	return !strings.ContainsAny(s, ".*> \t\r\n")
}

// Subject returns the subject e is published on in group,
// ntf.events.<group>.<instance>.<category>.<action>, built from the event's
// own instance id, category and action.
func Subject(group string, e Event) (string, error) {
	err := ValidateGroup(group)
	if err != nil {
		return "", err
	}

	instance := e.EventBase().InstanceID
	category := string(e.EventCategory())
	action := e.EventAction()
	for _, token := range []string{instance, category, action} {
		if !validToken(token) {
			return "", fmt.Errorf("%w: instance %q, category %q, action %q", ErrInvalidSubject, instance, category, action)
		}
	}

	return fmt.Sprintf("%s.%s.%s.%s.%s", subjectPrefix, group, instance, category, action), nil
}

// StreamSubject returns the subject the events stream of group captures,
// ntf.events.<group>.>.
func StreamSubject(group string) string {
	return fmt.Sprintf("%s.%s.>", subjectPrefix, group)
}

// InstanceFilter returns the subject filter matching every event of instance
// in group, ntf.events.<group>.<instance>.>.
func InstanceFilter(group string, instance string) string {
	return fmt.Sprintf("%s.%s.%s.>", subjectPrefix, group, instance)
}

// StreamName returns the name of the events stream of group, NTF_EVENTS_
// followed by the group in upper case.
func StreamName(group string) string {
	return streamPrefix + strings.ToUpper(group)
}
