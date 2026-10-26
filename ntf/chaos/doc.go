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

// Package chaos holds the public types of the ntf chaos engine: the run
// definition, the kinds and drivers a run's workers pair, the target a kind
// acts on, and the selectors and credentials a definition carries. The engine
// itself, its service and the kinds and drivers it registers live outside this
// package, so everything a kind or a driver needs is exported here.
//
// A run definition holds workers. Each worker pairs a Driver, which decides
// when to act, with a Kind, which decides what to do and on which targets:
//
//	duration: 30m
//	quiet:
//	  min: 30s
//	  max_fault: 2m
//	credentials:
//	  USERS1:
//	    user_password: {user: user1, password: password}
//	workers:
//	  - name: kicks
//	    driver:
//	      interval: {every: 20s, jitter: 2s}
//	    action:
//	      client_disconnect: {mode: kick, clients: {fraction: 0.1}}
//
// The client_disconnect kind in the example arrives in a later release; a
// chaos service's chaos.info and chaos.schema list the kinds it has.
//
// The driver and the action are written in the keyed form: an object whose
// single key names the driver or kind and whose value holds its params, which
// the named driver or kind decodes and checks against the schema it publishes.
//
// For each worker the engine calls Kind.New with the action's params, giving an
// Action, and Driver.New with the driver's params and a Clock, giving a
// Schedule. The worker then loops: Schedule.Next waits until it is time to act
// and returns an Act, the worker calls Action.Apply with the run's Target and a
// StepReporter of its own, and holds any Effect the Apply returns until the
// Act's hold ends, a later Act reverts it, a quiet window opens or the run
// ends, when it calls Effect.Revert.
//
// Kinds and drivers are registered by name in a Registry the service owns;
// this package keeps no registry of its own.
package chaos
