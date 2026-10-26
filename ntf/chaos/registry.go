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
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
)

// Registry holds named factories, kinds or drivers. The service that creates
// one owns it; this package keeps none. It is safe for concurrent use. A
// Registry must be made with NewKindRegistry or NewDriverRegistry; the zero
// value is not usable.
type Registry[T any] struct {
	errName    error
	errUnknown error

	mu        sync.RWMutex
	factories map[string]T
}

// NewKindRegistry returns an empty registry of kinds. Its errors wrap
// ErrKindName and ErrUnknownKind.
func NewKindRegistry() *Registry[Kind] {
	return &Registry[Kind]{errName: ErrKindName, errUnknown: ErrUnknownKind, factories: map[string]Kind{}}
}

// NewDriverRegistry returns an empty registry of drivers. Its errors wrap
// ErrDriverName and ErrUnknownDriver.
func NewDriverRegistry() *Registry[Driver] {
	return &Registry[Driver]{errName: ErrDriverName, errUnknown: ErrUnknownDriver, factories: map[string]Driver{}}
}

// Register adds factory under name. It refuses an empty name, a name already
// registered and a nil factory.
func (r *Registry[T]) Register(name string, factory T) error {
	if name == "" {
		return fmt.Errorf("%w: empty name", r.errName)
	}
	if any(factory) == nil {
		return fmt.Errorf("%w: %q has a nil factory", r.errName, name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	_, exists := r.factories[name]
	if exists {
		return fmt.Errorf("%w: %q is already registered", r.errName, name)
	}
	r.factories[name] = factory

	return nil
}

// Lookup returns the factory registered under name. For a name not registered
// it returns an error listing the registered names.
func (r *Registry[T]) Lookup(name string) (T, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, ok := r.factories[name]
	if !ok {
		var zero T
		registered := "none"
		if len(r.factories) > 0 {
			registered = strings.Join(slices.Sorted(maps.Keys(r.factories)), ", ")
		}
		return zero, fmt.Errorf("%w: %q, registered: %s", r.errUnknown, name, registered)
	}

	return factory, nil
}

// Names returns the registered names, sorted.
func (r *Registry[T]) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return slices.Sorted(maps.Keys(r.factories))
}
