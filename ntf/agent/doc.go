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
Package agent defines the network agent of an ntf instance. An agent serves an
instance rather than fronting one server: it puts a proxy in front of a managed
server's client port that clients reach instead of the server, which can
capture the traffic passing through it and shape it.

Everything an agent needs is in this package. An implementation that lives
outside orbit.go imports it and nothing else from ntf.

# Running an agent

The service:

 1. calls a Factory once, with Options, to build a Provider;
 2. calls Provider.Start with a Spec for each instance that needs an agent,
    before the instance's servers start, and keeps the Agent it returns;
 3. once the servers run, calls ClientEdge.Front for each server: with
    Capture set for every server of an instance created with trace or proxy,
    whose clients the agent captures on the server's Spec.CaptureProxies
    endpoint, and with Capture false for every server of an instance created
    with toxics alone;
 4. calls Agent.Stop when the instance goes, or when its create fails;
 5. calls Provider.Close once, after every instance is gone and before the
    management server stops.

# Plain values

Every Agent method takes and returns plain values, so each can become a NATS
request and reply, and an agent can later run outside the service's process. The
local resources in Options are for an agent running in the service's process;
a remote agent ignores them.

The agent owns any scratch space it needs.

# Faulting links

An agent that declares toxics implements Toxics, which the service forwards
tester.toxic.add, remove, clear and report to. Callers name links with an
api.ToxicSelector, never with a proxy's dial direction, and every agent must
resolve a selector the same way:

  - The upstream of a proxy carries what its dialer sends, the downstream what
    the dialer receives.
  - On a route, the data from node A to node B is the upstream of the A to B
    proxy plus the downstream of the B to A proxy, since the pooled route
    connections carry data both ways and either side may have dialed.
  - On a gateway, the proxy from cluster C to node N is shared by every node of
    C, and messages travel on the sender's outbound connection only, so the
    data from C to N is the upstream of that proxy. Only a cut acts on its
    downstream, which carries interest. A selector whose from is a node acts
    on its cluster's proxies.
  - On a client link, the data to the server is the upstream.
  - A node alone selects every route, gateway and client link of the node,
    both ways.
  - A cut always acts on both streams, and one-way cuts are refused. A cut
    that names a node silences that node's connections through the gateway
    proxies its cluster shares, existing and new, telling them apart by the
    server_name the dialer's INFO gives, which is Spec.Nodes' Name, so a cut of
    a node alone unplugs it.

# Growing the agent

Agent does not grow. Later capabilities arrive as separate interfaces an agent
also implements, declared through a capability list. ClientEdge, which capture
declares, is the first, and Toxics, which toxics declares, the second.

Provider.Capabilities is that list: capture, shaping and toxics so far, each a
Capability constant. The service decides what a provider supports from this
list alone, not by probing its agents: it refuses a create that needs a
capability the provider does not declare, and reports the list in status as
agent_capabilities, beside the backend's capabilities. A provider declares only
what its agents do: one that declares shaping answers the shaping methods, and
one that does not returns ErrUnsupported from them.
*/
package agent
