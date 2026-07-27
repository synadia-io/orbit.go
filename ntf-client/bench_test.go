package ntf

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// The benchmarks in this file exist as much to keep the testing.TB signatures
// honest as to measure anything: if a helper ever regresses to *testing.T this
// file stops compiling. They also demonstrate the intended shape — the outer
// *testing.B is captured by the closure, because the callback only receives a
// testing.TB.

// BenchmarkClusterRequestReply measures core NATS request/reply against a
// three-node managed cluster. Cluster setup happens outside the timed region.
func BenchmarkClusterRequestReply(b *testing.B) {
	client := New(b, testServerURL())
	defer client.Close(b)

	client.WithCluster(b, 3, func(tb testing.TB, nc *nats.Conn, inst *Instance) {
		sub, err := nc.Subscribe("bench.echo", func(m *nats.Msg) {
			_ = m.Respond(m.Data)
		})
		if err != nil {
			tb.Fatalf("could not subscribe: %v", err)
		}
		defer func() { _ = sub.Unsubscribe() }()

		if err := nc.Flush(); err != nil {
			tb.Fatalf("could not flush: %v", err)
		}

		b.ResetTimer()
		for b.Loop() {
			if _, err := nc.Request("bench.echo", []byte("x"), time.Second); err != nil {
				b.Fatalf("request failed: %v", err)
			}
		}
	})
}

// BenchmarkInstanceStatus measures the management round trip behind
// Instance.Status, which is the hot path for tests that poll an instance while
// stopping and starting its servers.
func BenchmarkInstanceStatus(b *testing.B) {
	client := New(b, testServerURL())
	defer client.Close(b)

	client.WithJetStreamServer(b, func(tb testing.TB, _ *nats.Conn, inst *Instance) {
		b.ResetTimer()
		for b.Loop() {
			_ = inst.Status(tb)
		}
	})
}
