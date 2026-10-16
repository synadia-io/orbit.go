package ntf

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestWithToxicsOption pins that WithToxics sets the toxics option, which is
// off without it.
func TestWithToxicsOption(t *testing.T) {
	co := resolveCreateOptions([]CreateOption{WithToxics()})
	if !co.toxics {
		t.Fatal("WithToxics did not set toxics")
	}

	plain := resolveCreateOptions(nil)
	if plain.toxics {
		t.Fatal("toxics set without WithToxics")
	}
}

// TestWithToxicsRefusedWithoutToxicsAgent pins that every kind of create sends
// toxics, by way of the service's refusal: ntf-server's network agent does not
// declare the toxics capability yet, so the service answers ErrService naming
// it.
func TestWithToxicsRefusedWithoutToxicsAgent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer m.Close()

	tests := []struct {
		name   string
		create func() (*Instance, error)
	}{
		{"server", func() (*Instance, error) {
			return m.CreateServer(ctx, false, WithToxics(), WithDescription(t.Name()))
		}},
		{"cluster", func() (*Instance, error) {
			return m.CreateCluster(ctx, 2, false, WithToxics(), WithDescription(t.Name()))
		}},
		{"super-cluster", func() (*Instance, error) {
			return m.CreateSuperCluster(ctx, 2, 2, false, WithToxics(), WithDescription(t.Name()))
		}},
	}

	for _, tt := range tests {
		inst, err := tt.create()
		if err == nil {
			inst.Destroy(t)
			t.Errorf("%s: created with toxics, want a refusal", tt.name)
			continue
		}
		if !errors.Is(err, ErrService) {
			t.Errorf("%s: err = %v, want ErrService", tt.name, err)
		}
		if !strings.Contains(err.Error(), `"toxics"`) {
			t.Errorf("%s: error %q does not name the toxics capability", tt.name, err)
		}
	}
}
