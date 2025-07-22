package counters

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"math/big"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
)

// NewCounterFromStream wraps an existing JetStream stream with counter functionality.
func NewCounterFromStream(js jetstream.JetStream, stream jetstream.Stream) (Counter, error) {
	info := stream.CachedInfo()
	if !info.Config.AllowMsgCounter {
		return nil, ErrCounterNotEnabled
	}
	if !info.Config.AllowDirect {
		return nil, ErrDirectAccessRequired
	}

	return &streamCounter{
		stream: stream,
		js:     js,
	}, nil
}

// GetCounter is a convenience function that retrieves a stream by name and wraps it as a counter.
// Returns an error if the stream doesn't exist or isn't configured for counters.
func GetCounter(ctx context.Context, js jetstream.JetStream, name string) (Counter, error) {
	stream, err := js.Stream(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, ErrCounterNotFound
		}
		return nil, fmt.Errorf("failed to get stream: %w", err)
	}

	return NewCounterFromStream(js, stream)
}

// Counter provides operations on a JetStream stream configured for distributed counters.
// Each subject in the stream is a separate counter.
type Counter interface {
	// Add increments the counter for the given subject and returns the new total value.
	Add(ctx context.Context, subject string, value *big.Int) (*big.Int, error)

	// AddInt increments the counter for the given subject and returns the new total value.
	AddInt(ctx context.Context, subject string, value int) (*big.Int, error)

	// Load returns the current value of the counter for the given subject.
	Load(ctx context.Context, subject string) (*Value, error)

	// LoadMultiple returns an iterator over counter values for multiple subjects.
	// Wildcards are supported.
	LoadMultiple(ctx context.Context, subjects []string) iter.Seq2[*Value, error]

	// GetEntry returns the full entry with value and source history for the given subject.
	GetEntry(ctx context.Context, subject string) (*Entry, error)

	// GetEntries returns an iterator over counter entries matching the pattern.
	// Wildcards are supported.
	GetEntries(ctx context.Context, subjects []string) iter.Seq2[*Entry, error]
}

type Value struct {
	// Val is the current value of the counter.
	Val *big.Int

	// Subject is the subject of the counter.
	Subject string
}

// Entry represents a counter's current state with full source history.
type Entry struct {
	// Subject is the counter's subject name
	Subject string

	// Value is the current counter value
	Value *big.Int

	// Sources maps source identifiers to their subject-value contributions.
	Sources CounterSources
}

// CounterSources is a map of source streams to their subject-value contributions.
type CounterSources map[string]map[string]*big.Int

type streamCounter struct {
	stream jetstream.Stream
	js     jetstream.JetStream
}

const (
	// CounterSourcesHeader is the header key used to store source contributions in counter messages.
	CounterSourcesHeader = "Nats-Counter-Sources"
	// CounterIncrementHeader is the header key used to indicate counter increments.
	CounterIncrementHeader = "Nats-Incr"
)

func (c *streamCounter) Add(ctx context.Context, subject string, value *big.Int) (*big.Int, error) {
	if value == nil {
		return nil, ErrInvalidCounterValue
	}

	msg := nats.NewMsg(subject)

	msg.Header.Set(CounterIncrementHeader, value.String())

	pubAck, err := c.js.PublishMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish counter increment: %w", err)
	}

	if pubAck.Value == "" {
		return nil, fmt.Errorf("counter increment response missing value")
	}

	result := &big.Int{}
	if _, ok := result.SetString(pubAck.Value, 10); !ok {
		return nil, fmt.Errorf("invalid counter value in response: %s", pubAck.Value)
	}
	return result, nil
}

func (c *streamCounter) AddInt(ctx context.Context, subject string, value int) (*big.Int, error) {
	val := big.NewInt(int64(value))
	return c.Add(ctx, subject, val)
}

func (c *streamCounter) Load(ctx context.Context, subject string) (*Value, error) {
	msg, err := c.stream.GetLastMsgForSubject(ctx, subject, jetstream.WithGetLastForSubjectNoHeaders())
	if err != nil {
		if err == jetstream.ErrMsgNotFound {
			return nil, fmt.Errorf("%w: %s", ErrNoCounterForSubject, subject)
		}
		return nil, fmt.Errorf("failed to get last message for subject %s: %w", subject, err)
	}

	val, err := parseCounterValue(msg.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse counter value for subject %s: %w", subject, err)
	}
	return &Value{
		Val:     val,
		Subject: msg.Subject,
	}, nil
}

func (c *streamCounter) LoadMultiple(ctx context.Context, subjects []string) iter.Seq2[*Value, error] {
	return func(yield func(*Value, error) bool) {
		streamName := c.stream.CachedInfo().Config.Name
		vals, err := jetstreamext.GetLastMsgsFor(ctx, c.js, streamName, subjects)
		if err != nil {
			yield(nil, err)
			return
		}
		for msg, err := range vals {
			if err != nil {
				yield(nil, err)
				continue
			}
			value, err := parseCounterValue(msg.Data)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(&Value{value, msg.Subject}, nil) {
				return
			}
		}
	}
}

func (c *streamCounter) GetEntry(ctx context.Context, subject string) (*Entry, error) {
	msg, err := c.stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		if err == jetstream.ErrMsgNotFound {
			return nil, ErrNoCounterForSubject
		}
		return nil, fmt.Errorf("failed to get last message for subject %s: %w", subject, err)
	}

	var value *big.Int

	value, err = parseCounterValue(msg.Data)
	if err != nil {
		return nil, err
	}

	sources, err := parseSources(msg.Header)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sources: %w", err)
	}

	return &Entry{
		Subject: subject,
		Value:   value,
		Sources: sources,
	}, nil
}

func (c *streamCounter) GetEntries(ctx context.Context, subjects []string) iter.Seq2[*Entry, error] {
	return func(yield func(*Entry, error) bool) {
		streamName := c.stream.CachedInfo().Config.Name
		vals, err := jetstreamext.GetLastMsgsFor(ctx, c.js, streamName, subjects)
		if err != nil {
			yield(nil, err)
			return
		}
		for msg, err := range vals {
			if err != nil {
				yield(nil, err)
				continue
			}
			value, err := parseCounterValue(msg.Data)
			if err != nil {
				yield(nil, fmt.Errorf("failed to parse counter value for subject %s: %w", msg.Subject, err))
				continue
			}
			sources, err := parseSources(msg.Header)
			if err != nil {
				yield(nil, fmt.Errorf("failed to parse sources: %w", err))
				continue
			}
			entry := &Entry{
				Subject: msg.Subject,
				Value:   value,
				Sources: sources,
			}
			if !yield(entry, nil) {
				return
			}
		}
	}
}

func parseCounterValue(data []byte) (*big.Int, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty counter value", ErrInvalidCounterValue)
	}
	var payload struct {
		Val string `json:"val"`
	}

	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal counter payload: %w", err)
	}

	value := new(big.Int)
	if _, ok := value.SetString(payload.Val, 10); !ok {
		return nil, fmt.Errorf("invalid counter value: %s", payload.Val)
	}

	return value, nil
}

func parseSources(headers nats.Header) (CounterSources, error) {
	sourcesHeader := headers.Get(CounterSourcesHeader)
	if sourcesHeader == "" {
		return nil, nil
	}

	var sources map[string]map[string]string
	err := json.Unmarshal([]byte(sourcesHeader), &sources)
	if err != nil {
		return nil, err
	}

	result := make(CounterSources, len(sources))
	for sourceID, source := range sources {
		result[sourceID] = make(map[string]*big.Int, len(source))
		for subject, valueStr := range source {
			value := new(big.Int)
			if _, ok := value.SetString(valueStr, 10); !ok {
				return nil, fmt.Errorf("invalid counter value for subject %s: %s", subject, valueStr)
			}
			result[sourceID][subject] = value
		}
	}
	return result, nil
}
