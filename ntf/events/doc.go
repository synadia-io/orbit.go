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
Package events defines the events an ntf service and its agents publish about
each instance, which every publisher and reader shares.

Events are flat JSON messages, typed and backed by a JSON Schema each, in the
style of jsm.go's advisories. Every event carries type, id (a nuid), timestamp
(RFC 3339 with nanoseconds), instance_id, source and source_seq beside the
fields of its type. A Go struct per type embeds Base, which holds them.

# Types

Each type id below is published on the subject shown, where <group> is the
service's group and <instance> the instance id, and has its schema at the path
shown inside this package, embedded and returned by Schema:

	io.synadia.ntf.event.v1.instance_action
	    ntf.events.<group>.<instance>.instance.<created|destroyed>
	    schemas/ntf/event/v1/instance_action.json

	io.synadia.ntf.event.v1.node_action
	    ntf.events.<group>.<instance>.node.<ready|stopped|started|updated|reloaded|killed|paused|resumed>
	    schemas/ntf/event/v1/node_action.json

	io.synadia.ntf.event.v1.shaping_action
	    ntf.events.<group>.<instance>.shaping.<applied|cleared>
	    schemas/ntf/event/v1/shaping_action.json

	io.synadia.ntf.event.v1.shaping_fired
	    ntf.events.<group>.<instance>.shaping.fired
	    schemas/ntf/event/v1/shaping_fired.json

	io.synadia.ntf.event.v1.capture_stored
	    ntf.events.<group>.<instance>.capture.stored
	    schemas/ntf/event/v1/capture_stored.json

	io.synadia.ntf.event.v1.toxic_action
	    ntf.events.<group>.<instance>.toxic.<added|removed|cleared>
	    schemas/ntf/event/v1/toxic_action.json

	io.synadia.ntf.event.v1.chaos_run
	    ntf.events.<group>.<instance>.chaos.<started|paused|resumed|stopped|finished>
	    schemas/ntf/event/v1/chaos_run.json

	io.synadia.ntf.event.v1.chaos_action
	    ntf.events.<group>.<instance>.chaos.<applied|step|apply_failed|reverted|revert_failed>
	    schemas/ntf/event/v1/chaos_action.json

	io.synadia.ntf.event.v1.chaos_window
	    ntf.events.<group>.<instance>.chaos.<opened|closed>
	    schemas/ntf/event/v1/chaos_window.json

A breaking change to a type is a new v2 id. An additive field is not, so the
schemas allow additional properties and an older schema accepts a newer event.
ParseEvent returns an *UnknownEvent for a type it does not know, so an older
reader survives types added later.

# Subjects and the stream

An event is published on ntf.events.<group>.<instance>.<category>.<action>, the
category and action coming from the event itself, as Subject builds it.
InstanceFilter matches every event of one instance, and a reader narrows by
category or action with wildcards.

Each service keeps one stream, named NTF_EVENTS_ followed by its group in upper
case, capturing ntf.events.<group>.>, as StreamName and StreamSubject return.
A group must be a single lowercase subject token valid in a stream name, which
ValidateGroup checks, so two services on one server never share a stream or
overlap in subjects.

# Sources and order

source names the component that published an event. It is service for the
instance and node events, which the ntf service publishes. It is agent for the
shaping, capture and toxic events, which a network agent publishes. It is chaos
for the chaos events, which a chaos engine publishes. backend is reserved.
source_seq counts from 1 per instance and source with no gaps, so a reader sees
one source's order directly and a dropped event shows as a gap.

The order of events is the stream sequence, total across the service and its
agents. timestamp is the publisher's clock, comparable across components within
the bound the clock check reports. The service publishes its events with an
ack. Agent events are not acked and may be lost. Chaos events may be lost too:
a chaos engine whose publish fails logs a warning and carries on with its run,
and the gap in source_seq shows the loss.

# Joins

Two joins to captures are exact and need no clock. shaping_fired's frame_id is
the id of the frame the rule acted on in the capture. connection_uuid is the
capture header's cuuid and the connection_uuid in its TRACES metadata. Field
names match the management API and the TRACES metadata, such as instance_id,
connection_uuid and server_name, so a reader joins without renaming.

# Clocks

Every component that stamps events answers a clock request on a micro endpoint
named clock, so a test can check that the components' clocks agree within a
bound it states. The request and reply are typed and schema-backed like the
events, but are not events, and Schema returns their schemas:

	io.synadia.ntf.clock.v1.request
	    schemas/ntf/clock/v1/request.json

	io.synadia.ntf.clock.v1.reply
	    schemas/ntf/clock/v1/reply.json

A component joins by adding a ClockHandler to its own micro service as the
endpoint named ClockEndpoint, under a group of its own, since a subject
answered by several processes mixes their clocks into one result. ProbeClock measures one participant's clock
against the local one: its offset, the round trip and an error bound of half
the round trip. The check confirms that every host runs NTP; it does not
replace it.

# Validation

Validate checks an event against its schema through a StructValidator, the
shape of jsm.go's, so this package carries no schema library.
*/
package events
