# Partitioned Consumer Groups

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/synadia-io/orbit.go/partitionedconsumergroups
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-io/orbit.go/partitionedconsumergroups
[Build-Status-Url]: https://github.com/synadia-io/orbit.go/actions/workflows/partitionedconsumergroups.yaml
[Build-Status-Image]: https://github.com/synadia-io/orbit.go/actions/workflows/partitionedconsumergroups.yaml/badge.svg?branch=main
[GoDoc-Url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/partitionedconsumergroups
[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/partitionedconsumergroups.svg

[![License][License-Image]][License-Url]
[![Go Reference][GoDoc-Image]][GoDoc-Url]
[![Build Status][Build-Status-Image]][Build-Status-Url]
[![Go Report Card][ReportCard-Image]][ReportCard-Url]

Initial implementation of a client-side partitioned consumer group feature for NATS streams leveraging some of the new features introduced in `nats-server` version 2.11.

Note that post 2.11 versions of `nats-server` may include new features related to the consumer group use case that could render this client-side library unneeded (or make much smaller) 
# Overview

This library enables the parallelization through partitioning of the consumption of messages from a stream while ensuring a strict order of not just delivery but also successful consumption of the messages using all or parts of the message's subject as a partitioning key.

In JetStream terms, strictly ordered consumption is achieved when you set the consumer's 'max acks pending' value to 1. However, setting this on a JetStream consumer has the very unfortunate side effect of being very low throughput (limited by the network latency and processing speed) and not being horizontally scalable: only one message is being delivered and processed synchronously at a time from that JetStream consumer, no matter how many instances of the consuming application are deployed.

The library allows the creation of 'consumer groups' on Stream, where each 'member' of the consumer group can consume from the group in parallel (with max acks pending 1 if needed), with the guarantee that in no way more than one message for a particular key can be consumed at the same time. Client applications wanting to consume messages from the group simply do so using a 'member name' and providing a callback. Even if more than one instance of a member is deployed, only one of those instances will be delivered messages at a time.

The library takes care of the partitioning and the mapping of the partitions between the members of the group, the idea being that it is mostly transparent to the consuming application's developers who only need to join a consumer group, providing a member name and a callback to process and acknowledge the message when successfully processed. 

NATS Partitioned consumer groups come in two flavors: *elastic* and *static*.

***Static*** partitioned consumer groups assume that the stream already has a partition number present as the first token of the message's subjects (something that can be done automatically when messages are stored into to the stream by setting a subject transform for the stream). You can only create and delete static consumer groups. Any change to the consumer group's config in the KV bucket will cause all the member instances for all members of the group to stop consuming.

***Elastic*** partitioned consumer groups on the other hand are implemented differently: the stream doesn't need to already contain a partition number subject token and you can administratively add and drop members from the consumer group's config whenever you want without having to delete and re-create the consumer (like you have to with static consumer groups).

***In both cases***
In both cases you must specify when creating the consumer group the maximum number of members for the group (which is actually the number of partitions used when partitioning the messages), plus a list of "members" (named instances of the consuming application). The library takes care of distributing the members over the list of partitions using either a 'balanced' distribution (the partitions are evenly distributed between the members) or 'mappings' (where you assign administratively the mappings of partitions to the members). The membership list or mappings must be specified once at consumer group creation time for static consumer groups, but can be changed at any time for elastic consumer groups.

Each consumer groups has a configuration which is stored in a KV bucket (named `static-consumer-groups` or `elastic-consumer-groups`).

## Static

Static consumer groups operate on a stream where the partition number has already been inserted in the subject as the first token of the messages. In this mode of operation, the library creates JetStream consumers (one per member of the group) directly on the stream. This is not elastic: you create the consumer with a list of members once, and you can not adjust that membership list or mapping for the life of the consumer group (if you want to change the mapping, up to you to delete and re-create the static partitioned consumer group, and to figure out which sequence number you may want this new static partitioned consumer group to start from).

## Elastic

Elastic consumer groups operate on any stream, the messages in the stream do not have the partition number present in their subjects. The membership list (or mapping) for the consumer can be adjusted administratively at any time and up to the max number of members defined initially. The consumer group library in this case creates a new work-queue stream that sources from the stream, inserting the partition number subject token on the way. The consumer group library takes care of creating this sourced stream and managing all the consumers on this stream according to the current membership, the developer only needs to provide a stream name, consumer group name and a member name and callback and make sure to ack the messages. You can specify (at creation time) a maximum size (in number of messages or bytes) for this working queue stream, but be aware that once this stream has reached its limit, it will pause the sourcing for at least 1 second (expecting messages to be consumed from the consumer group, thereby making room for more messages to be sourced) so you will want to set this value to more than 1 second's worth of message consumption by the clients of the consumer group or this could result in small delays in the consumption of messages from the consumer group.

## High availability

You can deploy and run multiple instances of the consuming application using the same member name, in that case only one of the running instances of the member will be 'pinned' and have messages delivered to it (thereby the other instances are effectively in hot standby). There are functions (`ElasticMemberStepDown()` and `StaticMemberStepDown`) to force a change of the currently pinned member instance.

## Using Partitioned Consumer Groups

For the client application programmer, there is one basic functionality exposed by both static and elastic partitioned consumer groups: join and consume messages (when selected) from a named consumer group on a stream by specifying a _member name_, a regular JetStream consumer config, and a _callback_. The library takes care of stripping the partition number token from the subject such that you can use any existing callback code you may already have as is.

There are also administrative functions to create and delete consumer groups, plus, in the case of elastic consumer groups only, the ability to add/drop members or to change the custom member to partition mappings on an existing elastic consumer group.

## CLI

Included is a small command line interface tool, located in the `cg` directory, that allows you to manage consumer groups, as well as test or demonstrate the functionality, and which can be registered as a plugin with the `nats` CLI tool (e.g. `nats plugins register cg /path/to/go/bin/cg`).

This `cg` CLI tool can be used by passing it commands and arguments directly, or with an interactive prompt using the `prompt` command (e.g. `cg static prompt`).

## Demo walkthrough

### Static

Create a stream "foo" that automatically partitions over 10 partitions using `static_stream_setup.sh`, then generate some traffic (a new message every 10ms) for that stream using `generate_traffic.sh`.

Create a static consumer group named "cg" on the stream in question, with two members defined called "m1" and "m2": `cg static create balanced foo cg 10 '>' m1 m2`

Start consuming messages with a simulated processing time of 20ms from an instance of member "m1": `cg static consume foo cg m1 --sleep 25ms`. Run in another window cg again to consume as member m2 a second, run multiple instances of members m1 and m2, kill the active one (the one receiving messages) and watch as one of the other instances takes over.

### Elastic

Create a stream 'foo' that captures messages on the subjects `foo.*`, then generate some traffic (a new message every 10ms) for that stream using `generate_traffic.sh`.

Create an elastic consumer group named "cg", partitioning over 10 partitions using the second token (first `*` wildcard in the filter "foo.*") in the subject as the partitioning key: `cg elastic create foo cg 10`.

At this point the elastic consumer group is created, but no members have been added to it yet. But you can start instances of your consuming members already (e.g. `cg elastic consume foo cg m1` for an instance of a member "m1"), for example start instances of members "m1", "m2" and "m3". At this point none of those members are receiving messages. 

Add "m1" and "m2" to the membership: `cg elastic add foo cg m1 m2`, see how they start receiving messages. Then drop "m1" from the membership `cg elastic drop foo cg m1`, add it again, and each time watch as the consumer starts and stops receiving messages, run another consumer "m3" and add/drop it from the membership, etc...

As soon as the elastic consumer group is created, you can start instances of consuming clients (e.g. `cg elastic consume foo cg m1`), and they will start to consume messages as soon as (and as long as) they are in the group's membership.

### Example

To start consuming from a static consumer group, you call `streamconsumergroup.StaticConsume`. To start consuming from an elastic consumer group you call `streamconsumergroup.ElasticConsume`. These calls will return an error and a `ConsumerGroupConsumeContext`. Assuming no error is returned,this will create a Go routine that handles consumption and monitoring for changes in the consumer group's config. 

e.g. for static
```golang
consumerGroupContext, err = streamconsumergroup.StaticConsume(myContext, nc, streamName, consumerGroupName, memberName, messageHandler, config)
```
The arguments are:
- `myContext` is a Golang `context.Context` which is going to be used only for the operations that are part of joining the consumer group. You must use `Stop()` on the `ConsumerGroupContext` being returned to stop the consumption.
- `nc` is a NATS connection object.
- `streamName` is the name of the Stream on which the consumer group has been created.
- `consumerGroupName` is the name of the consumer group that has been created on the stream.
- `memberName` is the name of the member you want to join the consumer group as.
- `messageHandler` is a callback function that gets invoked and passed the messages for consumption. Note that if you are using an elastic consumer group you _must_ explicitly acknowledge (positively or negatively) the message in your callback.
- `config` is a regular JetStream consumer config to use by the library as a template when actually creating the JetStream consumers. For elastic consumers the acknowledgement policy must be explicit. For static consumer groups, it doesn't have to, but if you want to do strictly one at a time processing, you will need to use explicit acks in order for max acks pending 1 to apply. Note that this configuration being used as template means that some of the values will be overwritten and can be left empty (e.g. names and durable names, filters, idle timeouts) or will be overwritten if they are too small (as there is a relationship that must be maintained between the ack wait time, the consumer fetch time out, and the pinned TTL values to avoid 'flapping' of the pinned client which means that lower values will cause in increase in load on the infrastructure because of the added overhead. At this point the ack wait time must be at least 6 seconds, but this may well change in the future and the community users are encouraged to give feed back on this implementation detail).

Consumption stops either when some error is encountered (for example, any change to the consumer group's config in the case of a static consumer group), the consumer group's config gets deleted, or you invoke `Stop()` on the `ConsumerGroupConsumeContext`. Invoking `Done()` on the `ConsumerGroupConsumeContext` will return a channel on which you can receive the error code indicating why the consumption was stopped. This error code will be `nil` if the consumption terminates normally (due to the consumer group getting deleted or `Stop()` being invoked on the `ConsumerGroupConsumeContext`).

You can look at the `cg` CLI tool's source code for examples of how to create and consume for both static and elastic consumer groups.
# Requirements

Note: partitioned consumer groups require NATS server version 2.11 or above.