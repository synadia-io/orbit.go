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

package agent

import "errors"

var (
	// ErrUnsupported means the agent cannot do what was asked, such as a
	// shaping call on an agent without shaping. An agent returns it, or an
	// error wrapping it.
	ErrUnsupported = errors.New("not supported by this agent")

	// ErrListen means the agent could not bind one of its listeners, usually
	// because another program took the port. Provider.Start and
	// ClientEdge.Front return an error wrapping it, and the service then runs
	// the create again with fresh ports.
	ErrListen = errors.New("could not listen")

	// ErrInvalidToxic means a toxics call was refused for what it asked: a
	// name that is malformed, in use or unknown, a selector that matches no
	// link, names an unknown node or cluster or contradicts itself, or a type,
	// attribute or toxicity the agent does not accept. An agent returns an
	// error wrapping it, whose text names the toxic and the field.
	ErrInvalidToxic = errors.New("invalid toxic")
)
