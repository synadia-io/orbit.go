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
