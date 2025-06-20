# NATS JetStream Extensions

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/synadia-io/orbit.go/jetstreamext
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-io/orbit.go/jetstreamext
[Build-Status-Url]: https://github.com/synadia-io/orbit.go/actions/workflows/jetstreamext.yaml
[Build-Status-Image]: https://github.com/synadia-io/orbit.go/actions/workflows/jetstreamext.yaml/badge.svg?branch=main
[GoDoc-Url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/jetstreamext
[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/jetstreamext.svg

[![License][License-Image]][License-Url]
[![Go Reference][GoDoc-Image]][GoDoc-Url]
[![Build Status][Build-Status-Image]][Build-Status-Url]
[![Go Report Card][ReportCard-Image]][ReportCard-Url]

JetStream Extensions is a set of utilities providing additional features to `jetstream` package in nats.go client.

## Installation

```bash
go get github.com/synadia-io/orbit.go/jetstreamext
```

## Utilities

### GetBatch and GetLastMsgsFor

`GetBatch` and `GetLastMsgsFor` are utilities that allow you to fetch multiple messages from a JetStream stream.
Responses are returned in an iterator, which you can range over to receive messages.

#### GetBatch

`GetBatch` fetches a `batch` of messages from a provided stream, starting from
either the lowest matching sequence, from the provided sequence, or from the
given time. It can be configured to fetch messages from matching subject (which
may contain wildcards) and up to a maximum byte limit.

Examples:

- fetching 10 messages from the beginning of the stream:

```go
msgs, err := jetstreamext.GetBatch(ctx, js, "stream", 10)
if err != nil {
    // handle error
}
for msg, err := range msgs {
    if err != nil {
        // handle error
    }
    fmt.Println(string(msg.Data))
}
```

- fetching 10 messages from the stream starting from sequence 100 and matching subject:

```go
msgs, err := jetstreamext.GetBatch(ctx, js, "stream", 10, jetstreamext.GetBatchSeq(100), jetstreamext.GetBatchSubject("foo"))
if err != nil {
    // handle error
}
// process msgs
```

- fetching 10 messages from the stream starting from time 1 hour ago:

```go
msgs, err := jetstreamext.GetBatch(ctx, js, "stream", 10, jetstreamext.GetBatchStartTime(time.Now().Add(-time.Hour)))
if err != nil {
    // handle error
}
// process msgs
```

- fetching 10 messages or up to provided byte limit:

```go
msgs, err := jetstreamext.GetBatch(ctx, js, "stream", 10, jetstreamext.GetBatchMaxBytes(1024))
if err != nil {
    // handle error
}
// process msgs
```

#### GetLastMsgsFor

`GetLastMsgsFor` fetches the last messages for the specified subjects from the specified stream. It can be optionally configured to fetch messages up to the provided sequence (or time), rather than the latest messages available. It can also be configured to fetch messages up to a provided batch size.
The provided subjects may contain wildcards, however it is important to note that the NATS server will match a maximum of 1024 subjects.

Responses are returned in an iterator, which you can range over to receive messages.

Examples:

- fetching last messages from the stream for the provided subjects:

```go
msgs, err := jetstreamext.GetLastMsgsFor(ctx, js, "stream", []string{"foo", "bar"})
if err != nil {
    // handle error
}
for msg, err := range msgs {
    if err != nil {
        // handle error
    }
    fmt.Println(string(msg.Data))
}
```

- fetching last messages from the stream for the provided subjects up to stream sequence 100:

```go
msgs, err := jetstreamext.GetLastMsgsFor(ctx, js, "stream", []string{"foo", "bar"}, jetstreamext.GetLastMsgsUpToSeq(100))
if err != nil {
    // handle error
}
// process msgs
```

- fetching last messages from the stream for the provided subjects up to time 1 hour ago:

```go
msgs, err := jetstreamext.GetLastMsgsFor(ctx, js, "stream", []string{"foo", "bar"}, jetstreamext.GetLastMsgsUpToTime(time.Now().Add(-time.Hour)))
if err != nil {
    // handle error
}
// process msgs
```

- fetching last messages from the stream for the provided subjects up to a batch size of 10:

```go
msgs, err := jetstreamext.GetLastMsgsFor(ctx, js, "stream", []string{"foo.*"}, jetstreamext.GetLastMsgsBatchSize(10))
if err != nil {
    // handle error
}
// process msgs
```
