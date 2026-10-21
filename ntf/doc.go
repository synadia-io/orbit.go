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
Package ntf runs NATS servers, clusters and super-clusters on demand, so tests can
talk to real servers over real connections instead of embedding one.

It hosts a NATS micro service. Clients ask it to create an instance, use the servers
it reports back, and destroy the instance when the test ends. The Go client for
driving it lives at https://github.com/synadia-io/orbit.go/tree/main/ntf-client;
other languages drive the subjects directly.

# Running one

The zero value starts an embedded server on an OS-chosen port and keeps instance
state in a temporary directory it removes on Close:

	svc, err := ntf.New(ctx, ntf.Options{})
	if err != nil {
		return err
	}
	defer svc.Close()

	c := ntfclient.New(t, svc.ClientURL())
	c.WithJetStreamCluster(t, 3, func(t testing.TB, nc *nats.Conn, inst *ntfclient.Instance) {
		// run tests against the cluster
	})

Pass Options.Conn to host the service on a connection you already have. Close leaves
that connection open, and ClientURL reports the URL it is connected to, so a client
reaches the service the same way in both modes.

# Managed servers

Managed servers listen on real TCP ports, which is the point: tests exercise the
network rather than an in-memory shortcut. They bind the wildcard address, so on a
developer machine an instance is reachable from the local network for as long as it
lives.

# Trace capture

Options.Agents registers network agent factories by name and Options.Agent selects
one. The agent, defined in package agent, fronts a managed server with a proxy
recording the traffic that reaches it. With no agent selected, requests asking for
trace capture are refused. Every node is fronted by a capture proxy of its own,
on a trace port from the service's range, and a traced instance cannot use
generated TLS: the proxy forwards plaintext.

# Clocks

Events from different components are lined up by their timestamps, so a
distributed deployment of ntf must run NTP on every host. The <group>.clock
endpoint confirms that rather than replacing it: it reports the service host as
the reference, each instance's agent as sharing the service's clock since it
runs in the service's process, and, for each extra clock subject the request
lists, that participant's offset from the service host with a round trip and an
error bound, or the error if it does not answer. The caller judges the result
against its own threshold.

A component joins by answering clock requests on its own micro service, adding
events.ClockHandler as the endpoint named clock under a group of its own, and
the caller lists that endpoint's subject. A subject answered by several
processes reaches one of them per request, mixing their clocks into one result.

# What embedding this grants

A client that can publish to the service's subjects can supply the whole
nats-server configuration for an instance, through Options that name a template or
config snippets. That is enough to write files anywhere the process can write, bind
any address, and read files back through config includes. The create response hands
back the private key of the generated TLS client certificate, managed servers carry
well-known credentials from the built-in template, and captured traces contain the
CONNECT frames of every connection, credentials included. Any client may destroy any
instance; there is no tenancy, quota or rate limit.

Treat running a Service as granting code execution on its host. On a shared server,
put it in a dedicated account and scope subject permissions to its group. The service
also needs ntf.events.<group>.> for its events and $JS.API.> for their stream.
Options.Group namespaces the endpoints but not micro's own $SRV discovery subjects, so
two services on one connection still collide there. On an unauthenticated server any
client can publish forged events or delete the events stream.
*/
package ntf
