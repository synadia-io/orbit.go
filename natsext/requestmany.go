package natsext

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/nats-io/nats.go"
)

type requestManyOpts struct {
	stall    time.Duration
	count    int
	sentinel func(*nats.Msg) bool
}

// RequestManyOpt is a function that can be used to configure the behavior of the RequestMany function.
type RequestManyOpt func(*requestManyOpts) error

// RequestManyStall sets the stall timer, which can be used in scatter-gather scenarios where subsequent
// responses are expected to arrive within a certain time frame.
func RequestManyStall(stall time.Duration) RequestManyOpt {
	return func(opts *requestManyOpts) error {
		if stall <= 0 {
			return fmt.Errorf("%w: stall time has to be greater than 0", nats.ErrInvalidArg)
		}
		opts.stall = stall
		return nil
	}
}

// RequestManyMaxMessages sets the maximum number of messages to receive.
func RequestManyMaxMessages(count int) RequestManyOpt {
	return func(opts *requestManyOpts) error {
		if count <= 0 {
			return fmt.Errorf("%w: expected request count has to be greater than 0", nats.ErrInvalidArg)
		}
		opts.count = count
		return nil
	}
}

// RequestManySentinel is a function that can be used to stop receiving messages
// once a sentinel message is received. A sentinel message is a message for
// which the provided function returns true.
func RequestManySentinel(f func(*nats.Msg) bool) RequestManyOpt {
	return func(opts *requestManyOpts) error {
		opts.sentinel = f
		return nil
	}
}

// DefaultSentinel is a sentinel function that stops receiving messages
// once a message with an empty payload is received.
func DefaultSentinel(msg *nats.Msg) bool {
	return len(msg.Data) == 0
}

// RequestMany will send a request payload and return an iterator to receive multiple responses.
// If context timeout is not set, the number of messages received is constrained by the client's timeout.
//
// Use the RequestManyOpt functions to further configure this method's behavior.
// - [RequestManyStall] sets the stall timer, which can be used in scatter-gather scenarios where subsequent
// responses are expected to arrive within a certain time frame.
// - [RequestManyMaxMessages] sets the maximum number of messages to receive.
// - [RequestManySentinel] stops returning responses once a message for which the provided function returns true.
func RequestMany(ctx context.Context, nc *nats.Conn, subject string, data []byte, opts ...RequestManyOpt) (iter.Seq2[*nats.Msg, error], error) {
	return requestMany(ctx, nc, subject, data, nil, opts...)
}

// RequestManyMsg will send a Msg request and return an iterator to receive multiple responses.
// If context timeout is not set, the number of messages received is constrained by the client's timeout.
//
// Use the RequestManyOpt functions to further configure this method's behavior.
// - [RequestManyStall] sets the stall timer, which can be used in scatter-gather scenarios where subsequent
// responses are expected to arrive within a certain time frame.
// - [RequestManyMaxMessages] sets the maximum number of messages to receive.
// - [RequestManySentinel] stops returning responses once a message for which the provided function returns true.
func RequestManyMsg(ctx context.Context, nc *nats.Conn, msg *nats.Msg, opts ...RequestManyOpt) (iter.Seq2[*nats.Msg, error], error) {
	if msg == nil {
		return nil, nats.ErrInvalidMsg
	}
	return requestMany(ctx, nc, msg.Subject, msg.Data, msg.Header, opts...)
}

func requestMany(ctx context.Context, nc *nats.Conn, subject string, data []byte, hdr nats.Header, opts ...RequestManyOpt) (iter.Seq2[*nats.Msg, error], error) {
	reqOpts := &requestManyOpts{
		count: -1,
	}

	for _, opt := range opts {
		if err := opt(reqOpts); err != nil {
			return nil, err
		}
	}

	inbox := nc.NewRespInbox()

	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}

	msg := &nats.Msg{
		Subject: subject,
		Reply:   inbox,
		Data:    data,
		Header:  hdr,
	}
	if err := nc.PublishMsg(msg); err != nil {
		return nil, err
	}
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, nc.Opts.Timeout)
	}

	return func(yield func(*nats.Msg, error) bool) {
		first := true
		completed := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
			case <-completed:
			}
		}()
		defer func() {
			close(completed)
			sub.Unsubscribe()
			if cancel != nil {
				cancel()
			}
		}()
		for {
			var timeout time.Duration
			if !first && reqOpts.stall != 0 {
				timeout = reqOpts.stall
			}
			first = false
			var msg *nats.Msg
			var err error
			if timeout == 0 {
				msg, err = sub.NextMsgWithContext(ctx)
			} else {
				msg, err = sub.NextMsg(timeout)
			}
			if err != nil {
				// ErrBadSubscription is returned if the subscription was closed by the global timeout
				if errors.Is(err, nats.ErrBadSubscription) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				yield(nil, err)
				return
			}
			if reqOpts.sentinel != nil && reqOpts.sentinel(msg) {
				return
			}
			if reqOpts.count >= 0 {
				reqOpts.count--
				if reqOpts.count < 0 {
					return
				}
			}
			if !yield(msg, nil) {
				return
			}
		}
	}, nil
}
