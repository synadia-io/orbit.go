package test

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
	"testing"
)

func TestAtomicBatchPublish(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "foo",
		// AllowAtomicPublish: true
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = jetstreamext.AtomicBatchPublish(ctx, nc, nil)
	if !errors.Is(err, jetstreamext.ErrAtomicBatchEmpty) {
		t.Fatal(err)
	}

	var msgs []*nats.Msg
	msgs = append(msgs, nats.NewMsg("foo"))
	msgs = append(msgs, nats.NewMsg("foo"))
	pubAck, err := jetstreamext.AtomicBatchPublish(ctx, nc, msgs)
	if err != nil {
		t.Fatal(err)
	}
	if pubAck.Sequence != 2 {
		t.Fatalf("expected sequence 2 on pub ack, got %d", pubAck.Sequence)
	}

	// Re-send the same batch.
	pubAck, err = jetstreamext.AtomicBatchPublish(ctx, nc, msgs)
	if err != nil {
		t.Fatal(err)
	}
	if pubAck.Sequence != 4 {
		t.Fatalf("expected sequence 4 on pub ack, got %d", pubAck.Sequence)
	}
}
