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

import "errors"

var (
	// ErrKindName means a kind cannot be registered under its name: the name
	// is empty or already registered, or the kind is nil.
	ErrKindName = errors.New("invalid kind name")

	// ErrUnknownKind means a name names no registered kind. The error lists
	// the registered names.
	ErrUnknownKind = errors.New("unknown kind")

	// ErrDriverName means a driver cannot be registered under its name: the
	// name is empty or already registered, or the driver is nil.
	ErrDriverName = errors.New("invalid driver name")

	// ErrUnknownDriver means a name names no registered driver. The error
	// lists the registered names.
	ErrUnknownDriver = errors.New("unknown driver")

	// ErrInvalidDefinition means a run definition could not be decoded or is
	// not valid. The error says which field and why.
	ErrInvalidDefinition = errors.New("invalid chaos definition")

	// ErrInvalidCredential means a credential is not valid, such as one with
	// no type or more than one.
	ErrInvalidCredential = errors.New("invalid credential")

	// ErrInvalidSelector means a selector is not valid, such as one with both
	// a count and a fraction.
	ErrInvalidSelector = errors.New("invalid selector")

	// ErrInvalidParams means the params of a driver or kind could not be
	// decoded.
	ErrInvalidParams = errors.New("invalid params")

	// ErrDriverDone means a schedule has no further acts. Schedule.Next
	// returns it once its driver is finished, such as a once driver after its
	// one act.
	ErrDriverDone = errors.New("driver has no further acts")

	// ErrTester means a tester.* request was answered with an error. A
	// Target returns it wrapped in a *TesterError, which holds the code.
	ErrTester = errors.New("tester request failed")
)
