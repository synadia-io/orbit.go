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

package ntf

import "errors"

var (
	// ErrInvalidPortRange means Options.PortRange is not a usable range: both ends
	// must lie in 1-65535 and Low must be below High.
	ErrInvalidPortRange = errors.New("invalid port range")

	// ErrPortRangeOverlapsEphemeral means the managed server port range reaches into
	// the range the OS draws ports for outgoing connections from, where any process
	// on the host can take a port between the service reserving it and a managed
	// server binding it.
	ErrPortRangeOverlapsEphemeral = errors.New("port range overlaps the OS ephemeral port range")

	// ErrPortRangeExhausted means every port in the range is either handed out to a
	// managed server or held by another program.
	ErrPortRangeExhausted = errors.New("no free port in the port range")

	// ErrServerFatal means a managed server reported a fatal error.
	ErrServerFatal = errors.New("managed server reported a fatal error")

	// ErrServerListen means a managed server could not bind one of its ports,
	// usually because another program took it after the service reserved it.
	ErrServerListen = errors.New("managed server could not listen")
)
