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

Initial implementation of a partitioned consumer group feature for NATS streams

# Overview

This client library implements the NATS equivalent to "key-partitioned consumer groups" in other streaming systems such as Apache Kafka.

Using partitioned consumer groups allows for the parallelization of strictly ordered consumption of the messages (if each consumer in the group is set with 'max acks pending' equal to 1) in a stream.

NATS Partitioned consumer groups come in two flavors: *elastic* and *static*.

***Static*** partitioned consumer groups assume that the stream already has a partition number present as the first token of the message's subjects. You can only create and delete static consumer groups. Any change to the consumer group's config in the KV bucket will cause all of the client instances for all members to stop consuming.

***Elastic*** partitioned consumer groups on the other hand are implemented differently but allow you to administratively add and drop members from the consumer group's config without having to delete and re-create the consumer.

In both cases you must specify when creating the consumer group the maximum number of members for the group (which is actually the number of partitions used when partitioning the messages), plus a list of "members" (named instances of the consuming application). The library takes care of distributing the members over the list of partitions using either a 'balanced' distribution (the partitions are evenly distributed between the members) or 'mappings' (where you assign administratively the partitions to the members). The membership list or mappings must be specified at consumer group creation time for static consumer groups, but can be changed at any time for elastic consumer groups. You can run multiple instances of a member at a time, only one of them will be 'pinned' and receive messages at a time.

Each consumer groups has a configuration which is stored in a KV bucket (named `static-consumer-groups` or `elastic-consumer-groups`).

## Static

Static consumer groups operate on a stream where the partition number has already been inserted in the subject as the first token of the messages. This is not elastic: you create the consumer with a list of members once, and you can not adjust that membership list or mapping for the life of the consumer group (if you want to change the mapping, up to you to delete and re-create the static partitioned consumer group, and to figure out which sequence number you may want this new static partitioned consumer group to start at).

## Elastic

Elastic consumer groups operate on any stream, the messages in the stream do not have the partition number present in their subjects. The membership list (or mapping) for the consumer can be adjusted administratively at any time and up to the max number of members defined initially. The consumer group in this case creates a new work-queue stream that sources from the stream, inserting the partition number subject token on the way. The consumer group takes care of creating this sourced stream and managing all the consumers on this stream according to the current membership, the developer only needs to provide a stream name, consumer group name and a member name and callback and make sure to ack the messages.

## Using Partitioned Consumer Groups

For the client application programmer, there is one basic functionality exposed by both static and elastic partitioned consumer groups: consume messages (when selected) from a named consumer group on a stream by specifying a _member name_ and a _callback_.

There also are administrative function to create and delete consumer groups, plus, in the case of elastic consumer groups only, the ability to add or drop members or to set a custom member to partition mapping.

## CLI

Included is a small command line interface tool, located in the `cg` directory, that allows you to manage consumer groups, as well as test or demonstrate the functionality, and which can be registered as a plugin with the `nats` CLI tool (using something like `nats plugins register cg /Users/jnmoyne/go/bin/cg`).

This `cg` CLI tool can be used by passing it commands and arguments directly, or with an interactive prompt using the `prompt` command (e.g. `cg static prompt`).

## Demo walkthrough

### Static

Create a stream "foo" that automatically partitions over 10 partitions using `static_stream_setup.sh`, then generate some traffic (a new message every 10ms) for that stream using `generate_traffic.sh`.

Create a static consumer group named "cg" on the stream in question, with two members defined called "m1" and "m2": `cg static create balanced foo cg 10 '>' m1 m2`

Start consuming messages with a simulated processing time of 20ms from an instance of member "m1": `cg static consume foo cg m1 --sleep 25ms`. Run in another window cg again to consume as member m2 a second, run multiple instances of members m1 and m2, kill the active one (the one receiving messages) and watch as one of the other instances takes over.

### Elastic

Create a stream 'foo' that captures messages on the subjects `foo.*`, then generate some traffic (a new message every 10ms) for that stream using `generate_traffic.sh`.

Create an elastic consumer group named "cg", partitioning over 10 partitions using the second token (first `*` wildcard in the filter "foo.*") in the subject as the partitioning key: `cg elastic create foo cg 10`.

At this point the elastic consumer group is created but no members have been added to it yet. But you can start instances of your consuming members already (e.g. `cg elastic consume foo cg m1` for an instance of a member "m1"), for example start instances of members "m1", "m2" and "m3". At this point none of those members are receiving messages. 

Add "m1" and "m2" to the membership: `cg elastic add foo cg m1 m2`, see how they start receiving messages. Then drop "m1" from the membership `cg elastic drop foo cg m1`, add it again, and each time watch as the consumer starts and stops receiving messages, run another consumer "m3" and add/drop it from the membership, etc...

# Requirements

Note: partitioned consumer groups require NATS server version 2.11 or above.