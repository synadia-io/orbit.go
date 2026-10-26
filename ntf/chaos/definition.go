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
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/parser"
)

const (
	// DefaultQuietMin is the shortest quiet window when a definition sets
	// none.
	DefaultQuietMin = 30 * time.Second

	// DefaultQuietMaxFault is the longest stretch before a quiet window is
	// due when a definition sets none.
	DefaultQuietMaxFault = 2 * time.Minute

	// MaxWorkerNameLength is the longest a worker name may be. Toxic names
	// are built from the run id, a nuid of 22 characters, and the worker
	// name, and hold at most 64 characters.
	MaxWorkerNameLength = 32
)

// workerName is what a worker name may hold: letters, digits, '_' and '-',
// which every toxic name and subject token accepts.
var workerName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Definition is a chaos run's definition, independent of the instance it is
// started on. It is JSON on the wire and YAML in a file. ParseDefinition and
// ParseDefinitionYAML decode it.
type Definition struct {
	// Duration is how long the run lasts. It is required.
	Duration Duration `json:"duration"`

	// Quiet sets the run's quiet windows.
	Quiet Quiet `json:"quiet,omitzero"`

	// Credentials are how the engine authenticates to each account, keyed by
	// account name, SystemAccount for the system account.
	Credentials map[string]Credential `json:"credentials,omitempty"`

	// Workers act concurrently through the run. At least one is required.
	Workers []Worker `json:"workers"`
}

// Quiet sets a run's quiet windows, the fault-free stretches a run
// guarantees. A zero field takes its default.
type Quiet struct {
	// Min is the shortest window, DefaultQuietMin when zero.
	Min Duration `json:"min,omitempty"`

	// MaxFault is the longest stretch before a window is due,
	// DefaultQuietMaxFault when zero.
	MaxFault Duration `json:"max_fault,omitempty"`
}

// Worker pairs a driver, which decides when to act, with a kind, which decides
// what to do.
type Worker struct {
	// Name names the worker, unique in its run: at most
	// MaxWorkerNameLength letters, digits, '_' or '-'.
	Name string `json:"name"`

	// Driver names the worker's driver and holds its params.
	Driver Spec `json:"driver"`

	// Action names the worker's kind and holds its params.
	Action Spec `json:"action"`
}

// Spec names a driver or kind and holds its params, in the keyed form: in JSON
// it is an object whose single key is the name and whose value, an object, is
// the params, such as {"interval": {"every": "20s"}}. A null value is read as
// an empty object.
type Spec struct {
	// Name is the name the driver or kind is registered under.
	Name string

	// Params are the driver's or kind's params, a JSON object.
	Params json.RawMessage
}

// MarshalJSON writes s in the keyed form.
func (s Spec) MarshalJSON() ([]byte, error) {
	params := s.Params
	if len(bytes.TrimSpace(params)) == 0 {
		params = json.RawMessage("{}")
	}

	return json.Marshal(map[string]json.RawMessage{s.Name: params})
}

// UnmarshalJSON reads s in the keyed form.
func (s *Spec) UnmarshalJSON(data []byte) error {
	var obj map[string]json.RawMessage
	err := json.Unmarshal(data, &obj)
	if err != nil {
		return fmt.Errorf("must be an object keyed by one name: %w", err)
	}
	if len(obj) != 1 {
		return fmt.Errorf("must be an object keyed by one name, got %d keys", len(obj))
	}

	for name, params := range obj {
		if name == "" {
			return errors.New("must be keyed by a name, got an empty key")
		}

		trimmed := bytes.TrimSpace(params)
		if bytes.Equal(trimmed, []byte("null")) {
			trimmed = []byte("{}")
		}
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return fmt.Errorf("params of %q must be an object", name)
		}

		*s = Spec{Name: name, Params: json.RawMessage(bytes.Clone(trimmed))}
	}

	return nil
}

// Duration is a time.Duration written as a Go duration string, such as "30s"
// or "1m30s".
type Duration time.Duration

// MarshalJSON writes d as a Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON reads d from a Go duration string.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return fmt.Errorf("duration must be a string such as \"30s\": %w", err)
	}

	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)

	return nil
}

// ParseDefinition decodes a definition from JSON, refusing unknown fields,
// field names in another case, such as "Duration", and duplicate keys, fills
// the defaults and validates it. The params of drivers and kinds are left to
// DecodeParams. Errors wrap ErrInvalidDefinition.
func ParseDefinition(data []byte) (*Definition, error) {
	err := refuseDuplicateKeys(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
	}

	var def Definition
	err = decodeStrict(data, &def)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
	}

	def.SetDefaults()

	err = def.Validate()
	if err != nil {
		return nil, err
	}

	return &def, nil
}

// ParseDefinitionYAML decodes a definition from YAML as ParseDefinition does
// from JSON, refusing unknown fields, duplicate keys and more than one YAML
// document.
func ParseDefinitionYAML(data []byte) (*Definition, error) {
	file, err := parser.ParseBytes(data, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
	}
	if len(file.Docs) > 1 {
		return nil, fmt.Errorf("%w: holds %d YAML documents, want one", ErrInvalidDefinition, len(file.Docs))
	}

	j, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
	}

	return ParseDefinition(j)
}

// SetDefaults fills the quiet window fields left zero with their defaults.
func (d *Definition) SetDefaults() {
	if d.Quiet.Min == 0 {
		d.Quiet.Min = Duration(DefaultQuietMin)
	}
	if d.Quiet.MaxFault == 0 {
		d.Quiet.MaxFault = Duration(DefaultQuietMaxFault)
	}
}

// Validate checks the definition's own fields: the durations, the
// credentials, and each worker's name, driver and action names. It does not
// check the drivers' and kinds' params, which their schemas check when a run
// starts. A zero quiet field is valid and means its default. Errors wrap
// ErrInvalidDefinition.
func (d *Definition) Validate() error {
	if d.Duration <= 0 {
		return fmt.Errorf("%w: duration must be above zero", ErrInvalidDefinition)
	}
	if d.Quiet.Min < 0 {
		return fmt.Errorf("%w: quiet.min must not be negative", ErrInvalidDefinition)
	}
	if d.Quiet.MaxFault < 0 {
		return fmt.Errorf("%w: quiet.max_fault must not be negative", ErrInvalidDefinition)
	}

	for account, cred := range d.Credentials {
		if account == "" {
			return fmt.Errorf("%w: credentials for an empty account name", ErrInvalidDefinition)
		}
		err := cred.Validate()
		if err != nil {
			return fmt.Errorf("%w: credentials for %q: %w", ErrInvalidDefinition, account, err)
		}
	}

	if len(d.Workers) == 0 {
		return fmt.Errorf("%w: at least one worker is required", ErrInvalidDefinition)
	}

	seen := map[string]bool{}
	for i, w := range d.Workers {
		if w.Name == "" {
			return fmt.Errorf("%w: worker %d has no name", ErrInvalidDefinition, i)
		}
		if len(w.Name) > MaxWorkerNameLength {
			return fmt.Errorf("%w: worker name %q is longer than %d characters", ErrInvalidDefinition, w.Name, MaxWorkerNameLength)
		}
		if !workerName.MatchString(w.Name) {
			return fmt.Errorf("%w: worker name %q may hold only letters, digits, '_' and '-'", ErrInvalidDefinition, w.Name)
		}
		if seen[w.Name] {
			return fmt.Errorf("%w: worker name %q is used twice", ErrInvalidDefinition, w.Name)
		}
		seen[w.Name] = true

		if w.Driver.Name == "" {
			return fmt.Errorf("%w: worker %q has no driver", ErrInvalidDefinition, w.Name)
		}
		if w.Action.Name == "" {
			return fmt.Errorf("%w: worker %q has no action", ErrInvalidDefinition, w.Name)
		}
	}

	return nil
}

// Redacted returns a copy of d with every credential redacted, to show
// wherever a definition is shown: reports, lists, events and logs. d is not
// changed.
func (d Definition) Redacted() Definition {
	out := d
	out.Workers = append([]Worker(nil), d.Workers...)

	if d.Credentials != nil {
		out.Credentials = make(map[string]Credential, len(d.Credentials))
		for account, cred := range d.Credentials {
			out.Credentials[account] = cred.Redacted()
		}
	}

	return out
}

// jsonFrame is an object or array open during refuseDuplicateKeys. keys holds
// the keys an object has had so far, and is nil for an array. wantKey says the
// object's next token is a key.
type jsonFrame struct {
	keys    map[string]bool
	wantKey bool
}

// refuseDuplicateKeys walks the first JSON value in data and refuses an object
// anywhere in it that holds a key twice, which decoding would otherwise read
// as the last of its values. It leaves anything after the value to
// decodeStrict.
func refuseDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	var open []*jsonFrame
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}

		if tok == json.Delim('}') || tok == json.Delim(']') {
			open = open[:len(open)-1]
			if len(open) == 0 {
				return nil
			}
			continue
		}

		if len(open) > 0 {
			top := open[len(open)-1]
			if top.keys != nil && top.wantKey {
				key, ok := tok.(string)
				if !ok {
					return fmt.Errorf("object key %v is not a string", tok)
				}
				if top.keys[key] {
					return fmt.Errorf("key %q appears more than once in an object", key)
				}
				top.keys[key] = true
				top.wantKey = false
				continue
			}
			if top.keys != nil {
				top.wantKey = true
			}
		}

		switch tok {
		case json.Delim('{'):
			open = append(open, &jsonFrame{keys: map[string]bool{}, wantKey: true})
		case json.Delim('['):
			open = append(open, &jsonFrame{})
		default:
			if len(open) == 0 {
				return nil
			}
		}
	}
}

// decodeStrict decodes the single JSON value in data into v, refusing fields v
// does not have, field names that differ from v's only in case, and anything
// after the value.
func decodeStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	err := dec.Decode(v)
	if err != nil {
		return err
	}

	_, err = dec.Token()
	if !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after the value")
	}

	return checkFieldNames(data, reflect.TypeOf(v))
}

var (
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
)

// checkFieldNames refuses an object key in data that is not the exact JSON
// name of a field of the struct t decodes it into. encoding/json matches field
// names without regard to case, so without this check "Duration" would decode
// as "duration". It looks into structs, maps, slices and arrays, and stops at
// a type that decodes itself, such as Spec, Duration or json.RawMessage.
func checkFieldNames(data []byte, t reflect.Type) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	var value any
	err := dec.Decode(&value)
	if err != nil {
		return err
	}

	return checkValueFieldNames(value, t)
}

// checkValueFieldNames checks value, decoded from JSON into a generic value,
// against t as checkFieldNames does.
func checkValueFieldNames(value any, t reflect.Type) error {
	if t == nil {
		return nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	ptr := reflect.PointerTo(t)
	if ptr.Implements(jsonUnmarshalerType) || ptr.Implements(textUnmarshalerType) {
		return nil
	}

	switch t.Kind() {
	case reflect.Struct:
		obj, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		fields := jsonFields(t)
		for _, key := range slices.Sorted(maps.Keys(obj)) {
			field, known := fields[key]
			if !known {
				for name := range fields {
					if strings.EqualFold(name, key) {
						return fmt.Errorf("unknown field %q, field names are case sensitive: %q", key, name)
					}
				}
				return fmt.Errorf("unknown field %q", key)
			}
			err := checkValueFieldNames(obj[key], field)
			if err != nil {
				return err
			}
		}

	case reflect.Map:
		obj, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		for _, key := range slices.Sorted(maps.Keys(obj)) {
			err := checkValueFieldNames(obj[key], t.Elem())
			if err != nil {
				return err
			}
		}

	case reflect.Slice, reflect.Array:
		items, ok := value.([]any)
		if !ok {
			return nil
		}
		for _, item := range items {
			err := checkValueFieldNames(item, t.Elem())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// jsonFields returns the types of the fields of the struct t by their JSON
// names, including those of embedded structs without a name of their own,
// which a field of t itself of the same name hides.
func jsonFields(t reflect.Type) map[string]reflect.Type {
	fields := map[string]reflect.Type{}
	var embedded []map[string]reflect.Type

	for f := range t.Fields() {
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")

		if f.Anonymous && name == "" {
			ft := f.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				embedded = append(embedded, jsonFields(ft))
				continue
			}
		}
		if !f.IsExported() {
			continue
		}
		if name == "" {
			name = f.Name
		}
		fields[name] = f.Type
	}

	for _, sub := range embedded {
		for name, ft := range sub {
			_, hidden := fields[name]
			if !hidden {
				fields[name] = ft
			}
		}
	}

	return fields
}
