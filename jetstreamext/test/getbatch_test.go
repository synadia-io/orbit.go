package test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

func TestGetBatch(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo.*"},
		AllowDirect: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	var pause time.Time

	// Publish some messages.
	for range 5 {
		if _, err := js.Publish(context.Background(), "foo.A", []byte("msg")); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
		if _, err := js.Publish(context.Background(), "foo.B", []byte("msg")); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	// pause here to test start time filter
	pause = time.Now()
	for range 5 {
		if _, err := js.Publish(context.Background(), "foo.A", []byte("msg")); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
		if _, err := js.Publish(context.Background(), "foo.B", []byte("msg")); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	tests := []struct {
		name         string
		batch        int
		opts         []jetstreamext.GetBatchOpt
		expectedMsgs int
		expectedSeqs []uint64
		withError    error
	}{
		{
			name:         "no options provided, get 10 messages",
			batch:        10,
			expectedMsgs: 10,
			expectedSeqs: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name:         "get 5 messages from sequence 5",
			batch:        5,
			opts:         []jetstreamext.GetBatchOpt{jetstreamext.GetBatchSeq(5)},
			expectedMsgs: 5,
			expectedSeqs: []uint64{5, 6, 7, 8, 9},
		},
		{
			name:         "get 5 messages from subject foo.B",
			batch:        5,
			opts:         []jetstreamext.GetBatchOpt{jetstreamext.GetBatchSubject("foo.B")},
			expectedMsgs: 5,
			expectedSeqs: []uint64{2, 4, 6, 8, 10},
		},
		{
			name:         "get 5 messages from sequence 5 and subject foo.B",
			batch:        5,
			opts:         []jetstreamext.GetBatchOpt{jetstreamext.GetBatchSeq(5), jetstreamext.GetBatchSubject("foo.B")},
			expectedMsgs: 5,
			expectedSeqs: []uint64{6, 8, 10, 12, 14},
		},
		{
			name:         "get more messages than available",
			batch:        10,
			opts:         []jetstreamext.GetBatchOpt{jetstreamext.GetBatchSeq(16)},
			expectedMsgs: 5,
			expectedSeqs: []uint64{16, 17, 18, 19, 20},
		},
		{
			name:         "with max bytes",
			batch:        10,
			opts:         []jetstreamext.GetBatchOpt{jetstreamext.GetBatchMaxBytes(15)},
			expectedMsgs: 2,
			expectedSeqs: []uint64{1, 2},
		},
		{
			name:  "seq higher than available",
			batch: 10,
			opts:  []jetstreamext.GetBatchOpt{jetstreamext.GetBatchSeq(21)},
		},
		{
			name:         "with start time",
			batch:        10,
			opts:         []jetstreamext.GetBatchOpt{jetstreamext.GetBatchStartTime(pause)},
			expectedMsgs: 10,
			expectedSeqs: []uint64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		},
		{
			name:      "start time and sequence exclusive",
			batch:     10,
			opts:      []jetstreamext.GetBatchOpt{jetstreamext.GetBatchStartTime(pause), jetstreamext.GetBatchSeq(5)},
			withError: jetstreamext.ErrInvalidOption,
		},
		{
			name:      "invalid max bytes",
			batch:     10,
			opts:      []jetstreamext.GetBatchOpt{jetstreamext.GetBatchMaxBytes(0)},
			withError: jetstreamext.ErrInvalidOption,
		},
		{
			name:      "invalid sequence",
			batch:     10,
			opts:      []jetstreamext.GetBatchOpt{jetstreamext.GetBatchSeq(0)},
			withError: jetstreamext.ErrInvalidOption,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			batch, err := jetstreamext.GetBatch(context.Background(), js, "TEST", test.batch, test.opts...)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error %v, got %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Failed to get batch: %v", err)
			}

			var i int
			for msg, err := range batch {
				if err != nil {
					if test.expectedMsgs == 0 && errors.Is(err, jetstreamext.ErrNoMessages) {
						break
					}
					t.Fatal(err)
				}
				if i >= len(test.expectedSeqs) {
					t.Fatalf("Received more messages than expected")
				}
				if msg.Sequence != test.expectedSeqs[i] {
					t.Fatalf("Expected sequence %d, got %d", test.expectedSeqs[i], msg.Sequence)
				}
				i++
			}
			if i != test.expectedMsgs {
				t.Fatalf("Expected %d messages, got %d", test.expectedMsgs, i)
			}
		})
	}
}

func TestGetLastMessagesFor(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo.*"},
		AllowDirect: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	pub := func(t *testing.T, subject string) {
		t.Helper()
		if _, err := js.Publish(context.Background(), subject, []byte("msg")); err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Publish some messages.
	// pub order (seq):
	// foo.A (1)
	// foo.A (2)
	// foo.B (3)
	// foo.A (4)
	// foo.B (5)
	// foo.B (6)
	// foo.C (7)
	pub(t, "foo.A")
	pub(t, "foo.A")
	pub(t, "foo.B")
	pub(t, "foo.A")
	pub(t, "foo.B")
	// pause here to test up_to_time
	pause := time.Now()
	time.Sleep(100 * time.Millisecond)
	pub(t, "foo.B")
	pub(t, "foo.C")

	tests := []struct {
		name         string
		subjects     []string
		opts         []jetstreamext.GetLastForOpt
		expectedMsgs int
		expectedSeqs []uint64
		withError    error
	}{
		{
			name:         "match all subjects",
			subjects:     []string{"foo.*"},
			expectedMsgs: 3,
			expectedSeqs: []uint64{4, 6, 7},
		},
		{
			name:         "match single subject",
			subjects:     []string{"foo.A"},
			expectedMsgs: 1,
			expectedSeqs: []uint64{4},
		},
		{
			name:         "match multiple subjects",
			subjects:     []string{"foo.A", "foo.B"},
			expectedMsgs: 2,
			expectedSeqs: []uint64{4, 6},
		},
		{
			name:         "match all up to sequence",
			subjects:     []string{"foo.*"},
			opts:         []jetstreamext.GetLastForOpt{jetstreamext.GetLastMsgsUpToSeq(3)},
			expectedMsgs: 2,
			expectedSeqs: []uint64{2, 3},
		},
		{
			name:         "match all up to time",
			subjects:     []string{"foo.*"},
			opts:         []jetstreamext.GetLastForOpt{jetstreamext.GetLastMsgsUpToTime(pause)},
			expectedMsgs: 2,
			expectedSeqs: []uint64{4, 5},
		},
		{
			name:         "with batch size",
			subjects:     []string{"foo.*"},
			opts:         []jetstreamext.GetLastForOpt{jetstreamext.GetLastMsgsBatchSize(2)},
			expectedMsgs: 2,
			expectedSeqs: []uint64{4, 6},
		},
		{
			name:         "no messages match filter",
			subjects:     []string{"foo.Z"},
			expectedMsgs: 0,
		},
		{
			name:      "empty subjects",
			subjects:  []string{},
			withError: jetstreamext.ErrSubjectRequired,
		},
		{
			name:      "time and sequence exclusive",
			subjects:  []string{"foo.*"},
			opts:      []jetstreamext.GetLastForOpt{jetstreamext.GetLastMsgsUpToTime(pause), jetstreamext.GetLastMsgsUpToSeq(3)},
			withError: jetstreamext.ErrInvalidOption,
		},
		{
			name:      "invalid batch size",
			subjects:  []string{"foo.*"},
			opts:      []jetstreamext.GetLastForOpt{jetstreamext.GetLastMsgsBatchSize(0)},
			withError: jetstreamext.ErrInvalidOption,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			batch, err := jetstreamext.GetLastMsgsFor(context.Background(), js, "TEST", test.subjects, test.opts...)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error %v, got %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Failed to get batch: %v", err)
			}

			var i int
			for msg, err := range batch {
				if err != nil {
					if test.expectedMsgs == 0 && errors.Is(err, jetstreamext.ErrNoMessages) {
						break
					}
					t.Fatal(err)
				}
				if i >= len(test.expectedSeqs) {
					t.Fatalf("Received more messages than expected")
				}
				if msg.Sequence != test.expectedSeqs[i] {
					t.Fatalf("Expected sequence %d, got %d", test.expectedSeqs[i], msg.Sequence)
				}
				i++
			}
			if i != test.expectedMsgs {
				t.Fatalf("Expected %d messages, got %d", test.expectedMsgs, i)
			}
		})
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

func shutdownJSServerAndRemoveStorage(t testing.TB, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}

func client(t testing.TB, s *server.Server, opts ...nats.Option) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return nc
}

func jsClient(t testing.TB, s *server.Server, opts ...nats.Option) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc := client(t, s, opts...)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}
