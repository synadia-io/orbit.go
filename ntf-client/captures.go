package ntf

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// captureWait bounds Captures when its context carries no deadline.
	captureWait = 10 * time.Second
	// capturePoll is how often Captures lists the store while it waits.
	capturePoll = 100 * time.Millisecond
)

// Capture is one connection's trace in the management server's TRACES object
// store, captured by the proxy of an instance created with WithTraceCapture.
type Capture struct {
	// Info is the stored object's info. Info.Metadata carries the keys the
	// capture proxy sets, among them client_name, instance_id, connection_uuid,
	// server_name, lang, version, captured_at, truncated, shaped,
	// shaping_firings and shaping_sets.
	Info *jetstream.ObjectInfo
	// CapturedAt is when the captured connection closed, from the captured_at
	// metadata.
	CapturedAt time.Time

	store jetstream.ObjectStore
}

// Read returns the capture's content, a trace in the Trace Assert expanded
// format (JSON Lines).
func (c *Capture) Read(ctx context.Context) ([]byte, error) {
	ctx, cancel := withDefaultTimeout(ctx, requestTimeout)
	defer cancel()

	data, err := c.store.GetBytes(ctx, c.Info.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrTraceStore, c.Info.Name, err)
	}
	return data, nil
}

// Captures returns the captures of the instance with the given ID whose CONNECT
// name is clientName, ordered by CapturedAt.
//
// A capture is stored only after the proxy sees its connection close, so a
// capture can land after the client has closed. Captures polls the store until at
// least want captures match and then returns every match, or returns
// ErrCaptureWait when ctx ends first. A want of zero or less returns the matches
// present now without waiting. With no deadline on ctx the wait is bounded at 10
// seconds.
func (m *Manager) Captures(ctx context.Context, instanceID string, clientName string, want int) ([]*Capture, error) {
	ctx, cancel := withDefaultTimeout(ctx, captureWait)
	defer cancel()

	store, err := m.TraceStore(ctx)
	if err != nil {
		return nil, err
	}

	found := 0
	for {
		captures, err := matchCaptures(ctx, store, instanceID, clientName)
		if err != nil && ctx.Err() == nil {
			return nil, err
		}
		if err == nil {
			found = len(captures)
			if found >= want {
				return captures, nil
			}
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: found %d of %d for %q: %w", ErrCaptureWait, found, want, clientName, ctx.Err())
		case <-time.After(capturePoll):
		}
	}
}

// matchCaptures lists store once and returns the objects of instanceID whose
// client_name is clientName, ordered by captured_at.
func matchCaptures(ctx context.Context, store jetstream.ObjectStore, instanceID string, clientName string) ([]*Capture, error) {
	infos, err := store.List(ctx)
	if errors.Is(err, jetstream.ErrNoObjectsFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTraceStore, err)
	}

	var captures []*Capture
	for _, info := range infos {
		if info.Metadata["instance_id"] != instanceID {
			continue
		}
		if info.Metadata["client_name"] != clientName {
			continue
		}

		at, err := time.Parse(time.RFC3339Nano, info.Metadata["captured_at"])
		if err != nil {
			return nil, fmt.Errorf("%w: %s: captured_at: %w", ErrInvalidCapture, info.Name, err)
		}
		captures = append(captures, &Capture{Info: info, CapturedAt: at, store: store})
	}

	slices.SortStableFunc(captures, func(a, b *Capture) int {
		return a.CapturedAt.Compare(b.CapturedAt)
	})
	return captures, nil
}
