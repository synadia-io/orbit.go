package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/natsext"
)

func TestRequestMany(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.Timeout(400*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	tests := []struct {
		name          string
		subject       string
		timeout       time.Duration
		opts          []natsext.RequestManyOpt
		minTime       time.Duration
		expectedMsgs  int
		withError     error
		withIterError error
	}{
		{
			name:         "default, context timeout",
			subject:      "foo",
			opts:         nil,
			minTime:      400 * time.Millisecond,
			expectedMsgs: 6,
		},
		{
			name:    "with stall, short circuit",
			subject: "foo",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManyStall(50 * time.Millisecond),
			},
			minTime:      50 * time.Millisecond,
			expectedMsgs: 5,
		},
		{
			name:         "with custom context",
			subject:      "foo",
			timeout:      500 * time.Millisecond,
			minTime:      500 * time.Millisecond,
			expectedMsgs: 6,
		},
		{
			name:    "with count reached",
			subject: "foo",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManyMaxMessages(3),
			},
			minTime:      0,
			expectedMsgs: 3,
		},
		{
			name:    "with custom timeout and limit",
			subject: "foo",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManyMaxMessages(3),
			},
			timeout:      500 * time.Millisecond,
			minTime:      0,
			expectedMsgs: 3,
		},
		{
			name:    "with count timeout",
			subject: "foo",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManyMaxMessages(10),
			},
			minTime:      400 * time.Millisecond,
			expectedMsgs: 6,
		},
		{
			name:    "sentinel",
			subject: "foo",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManySentinel(natsext.DefaultSentinel),
			},
			minTime:      100 * time.Millisecond,
			expectedMsgs: 5,
		},
		{
			name:    "all options provided, stall timer short circuit",
			subject: "foo",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManyStall(50 * time.Millisecond),
				natsext.RequestManyMaxMessages(10),
				natsext.RequestManySentinel(natsext.DefaultSentinel),
			},
			timeout:      500 * time.Millisecond,
			minTime:      50 * time.Millisecond,
			expectedMsgs: 5,
		},
		{
			name:    "all options provided, msg count short circuit",
			subject: "foo",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManyStall(50 * time.Millisecond),
				natsext.RequestManyMaxMessages(3),
				natsext.RequestManySentinel(natsext.DefaultSentinel),
			},
			timeout:      500 * time.Millisecond,
			minTime:      0,
			expectedMsgs: 3,
		},
		{
			name:    "all options provided, context short circuit",
			subject: "foo",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManyStall(100 * time.Millisecond),
				natsext.RequestManyMaxMessages(10),
				natsext.RequestManySentinel(natsext.DefaultSentinel),
			},
			timeout:      50 * time.Millisecond,
			minTime:      0,
			expectedMsgs: 5,
		},
		{
			name:    "all options provided, sentinel short circuit",
			subject: "foo",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManyStall(150 * time.Millisecond),
				natsext.RequestManyMaxMessages(10),
				natsext.RequestManySentinel(natsext.DefaultSentinel),
			},
			timeout:      500 * time.Millisecond,
			minTime:      100 * time.Millisecond,
			expectedMsgs: 5,
		},
		{
			name:    "with no responses",
			subject: "bar",
			// no responders
			withIterError: nats.ErrNoResponders,
		},
		{
			name: "invalid options - stall timer",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManyStall(-1),
			},
			subject:   "foo",
			withError: nats.ErrInvalidArg,
		},
		{
			name: "invalid options - max messages",
			opts: []natsext.RequestManyOpt{
				natsext.RequestManyMaxMessages(-1),
			},
			subject:   "foo",
			withError: nats.ErrInvalidArg,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := []byte("hello")
			for i := 0; i < 5; i++ {
				sub, err := nc.Subscribe("foo", func(m *nats.Msg) {
					time.Sleep(10 * time.Millisecond)
					nc.Publish(m.Reply, response)
				})
				if err != nil {
					t.Fatalf("Received an error on subscribe: %s", err)
				}
				defer sub.Unsubscribe()
			}
			// after short delay, send sentinel (should come in last)
			sub, err := nc.Subscribe("foo", func(m *nats.Msg) {
				time.Sleep(100 * time.Millisecond)
				nc.Publish(m.Reply, []byte(""))
			})
			if err != nil {
				t.Fatalf("Received an error on subscribe: %s", err)
			}
			defer sub.Unsubscribe()

			now := time.Now()
			var ctx context.Context
			if test.timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(context.Background(), test.timeout)
				defer cancel()
			} else {
				ctx = context.Background()
			}
			msgs, err := natsext.RequestMany(ctx, nc, test.subject, nil, test.opts...)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error %v, got %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Received an error on Request test: %s", err)
			}

			var i int
			for _, err := range msgs {
				if err != nil {
					if !errors.Is(err, test.withIterError) {
						t.Fatalf("Received an error on next: %s", err)
					}
					continue
				}
				i++
			}
			if i != test.expectedMsgs {
				t.Fatalf("Expected %d messages, got %d", test.expectedMsgs, i)
			}
			if time.Since(now) < test.minTime || time.Since(now) > test.minTime+100*time.Millisecond {
				t.Fatalf("Expected to receive all messages between %v and %v, got %v", test.minTime, test.minTime+100*time.Millisecond, time.Since(now))
			}
		})
	}
}

func TestRequestManyCancelContext(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.Timeout(400*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	sub, err := nc.Subscribe("foo", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("hello"))
	})
	if err != nil {
		t.Fatalf("Received an error on subscribe: %s", err)
	}
	defer sub.Unsubscribe()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	msgs, err := natsext.RequestMany(ctx, nc, "foo", nil)
	if err != nil {
		t.Fatalf("Received an error on Request test: %s", err)
	}

	var msgCount int
	for _, err := range msgs {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				if msgCount != 1 {
					t.Fatalf("Expected 1 message, got %d", msgCount)
				}
				return
			}
			t.Fatalf("Received unexpected error in iterator: %s", err)
		} else {
			msgCount++
		}
	}
	t.Fatalf("Expected context.Canceled error")
}

func TestRequestManySentinel(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.Timeout(400*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	sub, err := nc.Subscribe("foo", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("hello"))
		time.Sleep(10 * time.Millisecond)
		nc.Publish(m.Reply, []byte("world"))
		time.Sleep(10 * time.Millisecond)
		nc.Publish(m.Reply, []byte("goodbye"))
	})
	if err != nil {
		t.Fatalf("Received an error on subscribe: %s", err)
	}
	defer sub.Unsubscribe()

	expectedMsgs := []string{"hello", "world"}
	msgs, err := natsext.RequestMany(context.Background(), nc, "foo", nil, natsext.RequestManySentinel(func(m *nats.Msg) bool {
		return string(m.Data) == "goodbye"
	}))
	if err != nil {
		t.Fatalf("Received an error on Request test: %s", err)
	}

	got := make([]string, 0, 2)
	for msg, err := range msgs {
		if err != nil {
			t.Fatalf("Received an error on next: %s", err)
		}
		got = append(got, string(msg.Data))
	}
	if len(got) != len(expectedMsgs) {
		t.Fatalf("Expected %d messages, got %d", len(expectedMsgs), len(got))
	}

	for i, msg := range got {
		if msg != expectedMsgs[i] {
			t.Fatalf("Expected %s, got %s", expectedMsgs[i], msg)
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Running nats server in separate Go routines
////////////////////////////////////////////////////////////////////////////////

// RunDefaultServer will run a server on the default port.
func RunDefaultServer() *server.Server {
	return RunServerOnPort(nats.DefaultPort)
}

// RunServerOnPort will run a server on the given port.
func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.Cluster.Name = "testing"
	return RunServerWithOptions(&opts)
}

// RunServerWithOptions will run a server with the given options.
func RunServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}

// RunServerWithConfig will run a server with the given configuration file.
func RunServerWithConfig(configFile string) (*server.Server, *server.Options) {
	return natsserver.RunServerWithConfig(configFile)
}

func RunBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return RunServerWithOptions(&opts)
}
