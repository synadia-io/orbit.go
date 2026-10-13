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

/*
Package agent defines the network agent of an ntf instance: a proxy in front of
a managed server's client port that clients reach instead of the server, which
can capture the traffic passing through it and shape it.

Everything an agent needs is in this package. An implementation that lives
outside orbit.go imports it and nothing else from ntf.

# Running an agent

The service:

 1. calls a Factory once, with Options, to build a Provider;
 2. calls Provider.Start with a Spec for each instance that needs an agent,
    and keeps the Agent it returns;
 3. calls Agent.Stop when the instance goes;
 4. calls Provider.Close once, after every instance is gone and before the
    management server stops.

# Plain values

Every Agent method takes and returns plain values, so each can become a NATS
request and reply, and an agent can later run outside the service's process. The
local resources in Options are for an agent running in the service's process;
a remote agent ignores them.

The agent owns any scratch space it needs.

# Growing the agent

Agent does not grow. Later capabilities arrive as separate interfaces an agent
also implements, declared through a capability list.
*/
package agent
