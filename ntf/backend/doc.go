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
Package backend defines where the NATS servers of an ntf instance run. The ntf
service plans an instance, and a Backend places, starts, stops and removes its
servers: in the service's own process, in containers, in a cluster or anywhere
else an implementation reaches.

Everything a backend needs is in this package. An implementation that lives
outside orbit.go imports it and nothing else from ntf.

# Running an instance

For each instance the service:

 1. asks Backend.Placement for a Placement, and plans the instance against it:
    the endpoint of every listener, the instance directory and each node's store
    directory;
 2. hands the finished InstancePlan to Backend.Provision, which writes its files
    and starts its nodes, and keeps the Node values it returns;
 3. calls Backend.Teardown to remove the instance, whether it was destroyed, its
    plan failed, or Provision failed.

Backend.Close runs once, when the service closes.

# Plans

An InstancePlan and its NodePlan values are what a backend runs: names, rendered
configs, files and listener endpoints. They hold no template input; every file
arrives as rendered bytes. A plan holds private keys when the instance has
generated TLS, so a backend must not keep a plan, or any of its file contents,
once Provision or Node.Apply returns.

Paths in a plan, and the paths a Placement returns, are paths as the server sees
them, which is what its config renders. A backend that runs servers somewhere
other than its own host maps them to where it writes.
*/
package backend
