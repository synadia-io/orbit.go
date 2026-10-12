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

package backend

import "errors"

var (
	// ErrUnsupported means the backend cannot do what was asked, such as a
	// Node.Kill on a backend that does not declare CapabilityKill, or a
	// Node.Apply that changes the version on one that does not declare
	// CapabilityVersionChange. A backend returns it, or an error wrapping it.
	ErrUnsupported = errors.New("not supported by this backend")

	// ErrListen means a managed server could not bind one of its ports,
	// usually because another program took it after placement handed it out.
	// Backend.Provision returns an error wrapping it for such a failure, and
	// the service then plans the instance again with a fresh placement.
	ErrListen = errors.New("managed server could not listen")
)
