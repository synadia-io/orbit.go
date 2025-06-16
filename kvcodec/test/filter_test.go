package test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/kvcodec"
)

// NonFilterableCodec is a test codec that doesn't implement FilterableCodec
type NonFilterableCodec struct{}

func (n *NonFilterableCodec) EncodeKey(key string) (string, error) {
	return "encoded_" + key, nil
}

func (n *NonFilterableCodec) DecodeKey(key string) (string, error) {
	return strings.TrimPrefix(key, "encoded_"), nil
}

func (n *NonFilterableCodec) EncodeValue(value []byte) ([]byte, error) {
	return value, nil
}

func (n *NonFilterableCodec) DecodeValue(value []byte) ([]byte, error) {
	return value, nil
}

func TestFilterableCodec(t *testing.T) {
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

	ctx := context.Background()

	t.Run("FilterableCodec supports wildcards", func(t *testing.T) {
		// Create KV with filterable codec (Base64)
		baseKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket: "filterable_test",
		})
		if err != nil {
			t.Fatalf("create kv error: %v", err)
		}

		base64Codec := kvcodec.Base64Codec()
		kv := kvcodec.New(baseKV, base64Codec, base64Codec)

		// Should work with specific key
		watcher1, err := kv.Watch(ctx, "user.123")
		if err != nil {
			t.Errorf("Watch specific key failed: %v", err)
		} else {
			watcher1.Stop()
		}

		// Should work with wildcard
		watcher2, err := kv.Watch(ctx, "user.*")
		if err != nil {
			t.Errorf("Watch with wildcard failed: %v", err)
		} else {
			watcher2.Stop()
		}
	})

	t.Run("NonFilterableCodec rejects wildcards", func(t *testing.T) {
		// Create KV with non-filterable codec
		baseKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket: "non_filterable_test",
		})
		if err != nil {
			t.Fatalf("create kv error: %v", err)
		}

		nonFilterableCodec := &NonFilterableCodec{}
		kv := kvcodec.New(baseKV, nonFilterableCodec, nonFilterableCodec)

		// Should work with specific key
		watcher1, err := kv.Watch(ctx, "user.123")
		if err != nil {
			t.Errorf("Watch specific key failed: %v", err)
		} else {
			watcher1.Stop()
		}

		// Should fail with wildcard
		_, err = kv.Watch(ctx, "user.*")
		if err == nil {
			t.Error("Expected error for wildcard with non-filterable codec")
		} else if !errors.Is(err, kvcodec.ErrWildcardNotSupported) {
			t.Errorf("Expected ErrWildcardNotSupported, got: %v", err)
		}

		// Should fail with > wildcard
		_, err = kv.Watch(ctx, "user.>")
		if err == nil {
			t.Error("Expected error for > wildcard with non-filterable codec")
		} else if !strings.Contains(err.Error(), "does not support wildcard filtering") {
			t.Errorf("Expected wildcard filtering error, got: %v", err)
		}
	})
}
