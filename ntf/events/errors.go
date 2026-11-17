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

import "errors"

var (
	// ErrInvalidGroup means a group is not a single lowercase subject token
	// valid in a stream name: it must be made of a-z, 0-9, - and _ only.
	ErrInvalidGroup = errors.New("invalid events group")

	// ErrInvalidSubject means an event cannot be placed on a subject: its
	// instance id, category or action is empty or not a single subject token.
	ErrInvalidSubject = errors.New("invalid event subject")

	// ErrInvalidEvent means a message is not a JSON event, or an event does
	// not validate against its schema.
	ErrInvalidEvent = errors.New("invalid event")

	// ErrUnknownType means a type id names no event type, and no clock
	// request or reply, this package defines, so it has no schema.
	ErrUnknownType = errors.New("unknown event type")

	// ErrNoValidator means Validate was called without a validator.
	ErrNoValidator = errors.New("no validator given")

	// ErrClockProbe means a participant's clock could not be measured: its
	// subject is invalid, a request failed, or the reply was an error or not
	// a clock reply.
	ErrClockProbe = errors.New("clock probe failed")
)
