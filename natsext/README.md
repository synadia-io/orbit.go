# Core NATS Extensions

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
msgs, err := natsext.RequestMany(nc, "subject", []byte("request data"))
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
iter, err := natsext.RequestManyMsg(nc, msg)
if err != nil {
    // handle error
}
// gather responses
```

#### Options

Customize the termination behavior of `RequestMany` and `RequestManyMsg` using the following options:

- `RequestManyMaxWait`: Sets the maximum time to wait for responses (defaults to client's timeout).
- `RequestManyStall`: Sets the stall timer, useful in scatter-gather scenarios where subsequent responses are - expected within a certain timeframe.
- `RequestManyMaxMessages`: Sets the maximum number of messages to receive.
- `RequestManySentinel`: Stops receiving messages once a sentinel message is received.
