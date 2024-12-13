# Core NATS Extensions

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[ReportCard-Url]: https://goreportcard.com/report/github.com/synadia-io/orbit.go/natsext
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-io/orbit.go/natsext
[Build-Status-Url]: https://github.com/synadia-io/orbit.go/actions/workflows/natsext.yaml
[Build-Status-Image]: https://github.com/synadia-io/orbit.go/actions/workflows/natsext.yaml/badge.svg?branch=main
[GoDoc-Url]: https://pkg.go.dev/github.com/synadia-io/orbit.go/natsext
[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-io/orbit.go/natsext.svg

[![License][License-Image]][License-Url]
[![Go Reference][GoDoc-Image]][GoDoc-Url]
[![Build Status][Build-Status-Image]][Build-Status-Url]
[![Go Report Card][ReportCard-Image]][ReportCard-Url]

Core NATS Extensions is a set of utilities providing additional features to Core NATS component of nats.go client.

## Installation

```bash
go get github.com/synadia-io/orbit.go/natsext
```

## Utilities

### RequestMany

`RequestMany` is a utility that allows you to send a single request and await multiple responses.
This allows you to implement various patterns like scatter-gather or streaming responses.

Responses are returned in an iterator, which you can range over to receive messages.
When a termination condition is met, the iterator is closed (and no error is returned).

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
msgs, err := natsext.RequestMany(ctx, nc, "subject", []byte("request data"))
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

Alternatively, use `RequestManyMsg` to send a `nats.Msg` request:

```go
msg := &nats.Msg{
    Subject: "subject",
    Data:    []byte("request data"),
    Header:  nats.Header{
        "Key": []string{"Value"},
    },
}
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
iter, err := natsext.RequestManyMsg(ctx, nc, msg)
if err != nil {
    // handle error
}
// gather responses
```

#### Configuration

Timeout and cancellation are handled by the context passed to `RequestMany` and `RequestManyMsg`. In addition, you can configure the following options:

- `RequestManyStall`: Sets the stall timer, useful in scatter-gather scenarios where subsequent responses are expected within a certain timeframe.
- `RequestManyMaxMessages`: Sets the maximum number of messages to receive.
- `RequestManySentinel`: Stops receiving messages once a sentinel message is received.
