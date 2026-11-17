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
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

//go:embed schemas
var schemaFS embed.FS

// typePrefix is the part of every type id that is not in its schema path.
const typePrefix = "io.synadia."

// registry maps every type id this package defines to a constructor for its
// struct.
var registry = map[string]func() Event{
	TypeInstanceAction: func() Event { return &InstanceAction{} },
	TypeNodeAction:     func() Event { return &NodeAction{} },
	TypeShapingAction:  func() Event { return &ShapingAction{} },
	TypeShapingFired:   func() Event { return &ShapingFired{} },
	TypeCaptureStored:  func() Event { return &CaptureStored{} },
	TypeToxicAction:    func() Event { return &ToxicAction{} },
	TypeChaosRun:       func() Event { return &ChaosRun{} },
	TypeChaosAction:    func() Event { return &ChaosAction{} },
	TypeChaosWindow:    func() Event { return &ChaosWindow{} },
}

// clockTypes lists the type ids of the clock request and reply. They have
// schemas but are not events, so they are kept out of registry and ParseEvent
// does not know them.
var clockTypes = map[string]bool{
	TypeClockRequest: true,
	TypeClockReply:   true,
}

// UnknownEvent is what ParseEvent returns for a type this package does not
// know, so a reader survives types added after it was built. Base holds the
// common fields and Raw the whole message, which it marshals back to.
type UnknownEvent struct {
	Base

	Raw json.RawMessage `json:"-"`
}

func (e *UnknownEvent) EventType() string       { return e.Type }
func (e *UnknownEvent) EventCategory() Category { return "" }
func (e *UnknownEvent) EventAction() string     { return "" }

// MarshalJSON returns the message the event was parsed from.
func (e *UnknownEvent) MarshalJSON() ([]byte, error) {
	return e.Raw, nil
}

// ParseEvent reads the type of the JSON event data and returns it decoded into
// its struct, such as *NodeAction, or as *UnknownEvent for a type it does not
// know, including a message with no type.
func ParseEvent(data []byte) (Event, error) {
	var head *struct {
		Type string `json:"type"`
	}
	err := json.Unmarshal(data, &head)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidEvent, err)
	}
	if head == nil {
		return nil, fmt.Errorf("%w: null", ErrInvalidEvent)
	}

	factory, ok := registry[head.Type]
	if !ok {
		unknown := &UnknownEvent{Raw: append(json.RawMessage(nil), data...)}
		err = json.Unmarshal(data, &unknown.Base)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidEvent, err)
		}
		return unknown, nil
	}

	e := factory()
	err = json.Unmarshal(data, e)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidEvent, err)
	}
	return e, nil
}

// schemaPath returns the path of the JSON Schema for type id eventType, such
// as schemas/ntf/event/v1/node_action.json, inside this package's directory.
// eventType is an event type or a clock type.
func schemaPath(eventType string) (string, error) {
	_, ok := registry[eventType]
	if !ok && !clockTypes[eventType] {
		return "", fmt.Errorf("%w: %q", ErrUnknownType, eventType)
	}

	token := strings.TrimPrefix(eventType, typePrefix)
	return "schemas/" + strings.ReplaceAll(token, ".", "/") + ".json", nil
}

// Schema returns the JSON Schema document for type id eventType, an event type
// or the clock request or reply.
func Schema(eventType string) ([]byte, error) {
	path, err := schemaPath(eventType)
	if err != nil {
		return nil, err
	}

	return schemaFS.ReadFile(path)
}

// StructValidator validates a value against the JSON Schema of schemaType,
// returning the reasons it does not. It has the shape of jsm.go's
// validator.StructValidator, so this package carries no schema library. A
// validator looks schemas up with Schema, since jsm.go's api.Schema does not
// know these types.
type StructValidator interface {
	ValidateStruct(data any, schemaType string) (ok bool, errs []string)
}

// Validate validates e against the schema of its type with v. An event that
// does not validate is ErrInvalidEvent, carrying v's reasons.
func Validate(e Event, v StructValidator) error {
	if v == nil {
		return ErrNoValidator
	}

	ok, errs := v.ValidateStruct(e, e.EventType())
	if !ok {
		return fmt.Errorf("%w: %w", ErrInvalidEvent, errors.New(strings.Join(errs, ", ")))
	}
	return nil
}
