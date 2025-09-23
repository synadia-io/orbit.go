package test

import (
	"context"
	"errors"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/counters"
)

func TestCounterBasicOperations(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:            "TEST_COUNTER",
		Subjects:        []string{"foo.*"},
		AllowMsgCounter: true,
		AllowDirect:     true,
	})
	if err != nil {
		t.Fatalf("Failed to create counter stream: %v", err)
	}

	counter, err := counters.GetCounter(context.Background(), js, "TEST_COUNTER")
	if err != nil {
		t.Fatalf("Failed to get counter: %v", err)
	}

	// Test Add operation
	value, err := counter.Add(context.Background(), "foo.bar", big.NewInt(10))
	if err != nil {
		t.Fatalf("Failed to add to counter: %v", err)
	}
	if value.Cmp(big.NewInt(10)) != 0 {
		t.Fatalf("Expected value 10, got %s", value.String())
	}

	// Test Load operation
	loadedValue, err := counter.Load(context.Background(), "foo.bar")
	if err != nil {
		t.Fatalf("Failed to load counter: %v", err)
	}
	if loadedValue.Cmp(big.NewInt(10)) != 0 {
		t.Fatalf("Expected loaded value 10, got %s", loadedValue.String())
	}

	// Test adding more to the same counter
	value, err = counter.AddInt(context.Background(), "foo.bar", 5)
	if err != nil {
		t.Fatalf("Failed to add to counter: %v", err)
	}

	if value.Cmp(big.NewInt(15)) != 0 {
		t.Fatalf("Expected value 15, got %s", value.String())
	}

	// Test negative values (decrement)
	value, err = counter.Add(context.Background(), "foo.bar", big.NewInt(-3))
	if err != nil {
		t.Fatalf("Failed to add negative value: %v", err)
	}
	if value.Cmp(big.NewInt(12)) != 0 {
		t.Fatalf("Expected value 12, got %s", value.String())
	}

	// Verify final value with Load
	finalValue, err := counter.Load(context.Background(), "foo.bar")
	if err != nil {
		t.Fatalf("Failed to load final counter value: %v", err)
	}
	if finalValue.Cmp(big.NewInt(12)) != 0 {
		t.Fatalf("Expected final loaded value 12, got %s", finalValue.String())
	}

	// add 0
	value, err = counter.Add(context.Background(), "foo.bar", big.NewInt(0))
	if err != nil {
		t.Fatalf("Failed to add 0 to counter: %v", err)
	}
	if value.Cmp(big.NewInt(12)) != 0 {
		t.Fatalf("Expected value 12, got %s", value.String())
	}

	// try to add empty value
	_, err = counter.Add(context.Background(), "foo.bar", nil)
	if !errors.Is(err, counters.ErrInvalidCounterValue) {
		t.Fatalf("Expected ErrInvalidCounterValue, got %v", err)
	}
}

func TestCounterLoad(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:            "TEST_COUNTER",
		Subjects:        []string{"foo.*"},
		AllowMsgCounter: true,
		AllowDirect:     true,
	})
	if err != nil {
		t.Fatalf("Failed to create counter stream: %v", err)
	}

	counter, err := counters.GetCounter(context.Background(), js, "TEST_COUNTER")
	if err != nil {
		t.Fatalf("Failed to get counter: %v", err)
	}

	// Test Load operation for existing subject
	value, err := counter.Load(context.Background(), "foo.bar")
	if !errors.Is(err, counters.ErrNoCounterForSubject) {
		t.Fatalf("Expected ErrNoCounterForSubject, got %v", err)
	}
	if value != nil {
		t.Fatalf("Expected nil value for non-existent subject, got %s", value.String())
	}

	// Load a subject that has not been added yet
	_, err = counter.Load(context.Background(), "foo.baz")
	if !errors.Is(err, counters.ErrNoCounterForSubject) {
		t.Fatalf("Expected ErrNoCounterForSubject, got %v", err)
	}

	// Empty subject
	_, err = counter.Load(context.Background(), "")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
}

func TestCounterGetEntry(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:            "TEST_COUNTER",
		Subjects:        []string{"foo.*"},
		AllowMsgCounter: true,
		AllowDirect:     true,
	})
	if err != nil {
		t.Fatalf("Failed to create counter stream: %v", err)
	}

	counter, err := counters.GetCounter(context.Background(), js, "TEST_COUNTER")
	if err != nil {
		t.Fatalf("Failed to get counter: %v", err)
	}

	_, err = counter.Add(context.Background(), "foo.bar", big.NewInt(100))
	if err != nil {
		t.Fatalf("Failed to add to counter: %v", err)
	}

	entry, err := counter.Get(context.Background(), "foo.bar")
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if entry.Subject != "foo.bar" {
		t.Fatalf("Expected subject foo.bar, got %s", entry.Subject)
	}
	if entry.Value.Cmp(big.NewInt(100)) != 0 {
		t.Fatalf("Expected value 100, got %s", entry.Value.String())
	}
	if len(entry.Sources) != 0 {
		t.Fatalf("Expected no sources, got %d", len(entry.Sources))
	}

	if entry.Incr.Cmp(big.NewInt(100)) != 0 {
		t.Fatalf("Expected increment 100, got %s", entry.Incr.String())
	}

	_, err = counter.Get(context.Background(), "foo.nonexistent")
	if !errors.Is(err, counters.ErrNoCounterForSubject) {
		t.Fatalf("Expected ErrNoCounterForSubject, got %v", err)
	}

	// get entry with empty subject
	_, err = counter.Get(context.Background(), "")
	if err == nil {
		t.Fatal("Expected error for empty subject, got nil")
	}
}

func TestCounterGetMultiple(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:            "TEST_COUNTER_ENTRIES",
		Subjects:        []string{"foo.*"},
		AllowMsgCounter: true,
		AllowDirect:     true,
	})
	if err != nil {
		t.Fatalf("Failed to create counter stream: %v", err)
	}

	counter, err := counters.GetCounter(context.Background(), js, "TEST_COUNTER_ENTRIES")
	if err != nil {
		t.Fatalf("Failed to get counter: %v", err)
	}

	// Add to multiple subjects
	subjects := []string{"foo.one", "foo.two", "foo.three"}
	values := []*big.Int{big.NewInt(10), big.NewInt(20), big.NewInt(30)}

	for i, subject := range subjects {
		_, err := counter.Add(context.Background(), subject, values[i])
		if err != nil {
			t.Fatalf("Failed to add to %s: %v", subject, err)
		}
	}

	// Test GetEntries with wildcard
	count := 0
	foundSubjects := make(map[string]*big.Int)
	for entry, err := range counter.GetMultiple(context.Background(), []string{"foo.*"}) {
		if err != nil {
			t.Fatalf("error in GetEntries: %v", err)
		}
		foundSubjects[entry.Subject] = entry.Value
		count++
	}

	if count != 3 {
		t.Fatalf("Expected 3 entries matching app.*, got %d", count)
	}

	// Verify each subject was found with correct value
	for i, subject := range subjects {
		if value, found := foundSubjects[subject]; !found {
			t.Fatalf("subject %s not found in GetEntries results", subject)
		} else if value.Cmp(values[i]) != 0 {
			t.Fatalf("subject %s: expected %s, got %s", subject, values[i].String(), value.String())
		}
	}

	// Test GetEntries with specific pattern
	count = 0
	for entry, err := range counter.GetMultiple(context.Background(), []string{"foo.one"}) {
		if err != nil {
			t.Fatalf("error in GetEntries: %v", err)
		}
		if entry.Subject != "foo.one" {
			t.Fatalf("Expected subject foo.one, got %s", entry.Subject)
		}
		if entry.Value.Cmp(big.NewInt(10)) != 0 {
			t.Fatalf("Expected value 10, got %s", entry.Value.String())
		}
		count++
	}

	if count != 1 {
		t.Fatalf("Expected 1 entry matching app.requests, got %d", count)
	}

	// get entries with empty subject
	for entry, err := range counter.GetMultiple(context.Background(), []string{""}) {
		if err == nil {
			t.Fatalf("Expected error for empty subject, got entry: %v", entry)
		}
	}
	// no subjects
	for entry, err := range counter.GetMultiple(context.Background(), []string{}) {
		if err == nil {
			t.Fatalf("Expected error for empty subjects, got entry: %v", entry)
		}
	}

}

func TestNewCounterFromStream(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	t.Run("create counter from stream", func(t *testing.T) {
		stream, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
			Name:            "COUNTER_STREAM",
			Subjects:        []string{"counters.*"},
			AllowMsgCounter: true,
			AllowDirect:     true,
		})
		if err != nil {
			t.Fatalf("Failed to create counter stream: %v", err)
		}
		_, err = counters.NewCounterFromStream(js, stream)
		if err != nil {
			t.Fatalf("Failed to create counter from stream: %v", err)
		}
	})

	t.Run("get counter by name", func(t *testing.T) {
		_, err := counters.GetCounter(context.Background(), js, "COUNTER_STREAM")
		if err != nil {
			t.Fatalf("Failed to get counter by name: %v", err)
		}
	})

	t.Run("get non-existent counter", func(t *testing.T) {
		_, err = counters.GetCounter(context.Background(), js, "non-existent")
		if !errors.Is(err, counters.ErrCounterNotFound) {
			t.Fatalf("Expected ErrCounterNotFound, got %v", err)
		}
	})

	t.Run("stream without counter enabled", func(t *testing.T) {
		nonCounterStream, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
			Name:            "NON_COUNTER_STREAM",
			Subjects:        []string{"test.*"},
			AllowMsgCounter: false,
			AllowDirect:     true,
		})
		if err != nil {
			t.Fatalf("Failed to create non-counter stream: %v", err)
		}

		_, err = counters.NewCounterFromStream(js, nonCounterStream)
		if !errors.Is(err, counters.ErrCounterNotEnabled) {
			t.Fatalf("Expected ErrCounterNotEnabled, got %v", err)
		}
	})

	t.Run("stream without direct get", func(t *testing.T) {
		nonDirectStream, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
			Name:            "NON_DIRECT_STREAM",
			Subjects:        []string{"test.direct.*"},
			AllowMsgCounter: true,
			AllowDirect:     false,
		})
		if err != nil {
			t.Fatalf("Failed to create non-direct stream: %v", err)
		}
		_, err = counters.NewCounterFromStream(js, nonDirectStream)
		if !errors.Is(err, counters.ErrDirectAccessRequired) {
			t.Fatalf("Expected ErrDirectAccessRequired, got %v", err)
		}
	})
}

func TestCounterWithSources(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	// Create es stream
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:            "COUNT_ES",
		Subjects:        []string{"count.es.*"},
		AllowMsgCounter: true,
		AllowDirect:     true,
	})
	if err != nil {
		t.Fatalf("Failed to create Spanish counter stream: %v", err)
	}

	// Create pl stream
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:            "COUNT_PL",
		Subjects:        []string{"count.pl.*"},
		AllowMsgCounter: true,
		AllowDirect:     true,
	})
	if err != nil {
		t.Fatalf("Failed to create Spanish counter stream: %v", err)
	}

	// Create eu stream aggregating es and pl
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:            "COUNT_EU",
		Subjects:        []string{"count.eu.*"},
		AllowMsgCounter: true,
		AllowDirect:     true,
		Sources: []*jetstream.StreamSource{
			{
				Name: "COUNT_ES",
				SubjectTransforms: []jetstream.SubjectTransformConfig{
					{
						Source:      "count.es.>",
						Destination: "count.eu.>",
					},
				},
			},
			{
				Name: "COUNT_PL",
				SubjectTransforms: []jetstream.SubjectTransformConfig{
					{
						Source:      "count.pl.>",
						Destination: "count.eu.>",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create EU counter stream: %v", err)
	}

	// Create global aggregation stream with source from EU
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:            "COUNT_GLOBAL",
		Subjects:        []string{"count.*"},
		AllowMsgCounter: true,
		AllowDirect:     true,
		Sources: []*jetstream.StreamSource{
			{
				Name: "COUNT_EU",
				SubjectTransforms: []jetstream.SubjectTransformConfig{
					{
						Source:      "count.eu.>",
						Destination: "count.>",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create global counter stream: %v", err)
	}

	// Get counters for each level
	esCounter, err := counters.GetCounter(context.Background(), js, "COUNT_ES")
	if err != nil {
		t.Fatalf("Failed to get counter: %v", err)
	}

	plCounter, err := counters.GetCounter(context.Background(), js, "COUNT_PL")
	if err != nil {
		t.Fatalf("Failed to get counter: %v", err)
	}

	euCounter, err := counters.GetCounter(context.Background(), js, "COUNT_EU")
	if err != nil {
		t.Fatalf("Failed to get counter: %v", err)
	}

	globalCounter, err := counters.GetCounter(context.Background(), js, "COUNT_GLOBAL")
	if err != nil {
		t.Fatalf("Failed to get global counter: %v", err)
	}

	// Add to local counters
	_, err = esCounter.Add(context.Background(), "count.es.hits", big.NewInt(100))
	if err != nil {
		t.Fatalf("Failed to add to counter: %v", err)
	}

	_, err = esCounter.Add(context.Background(), "count.es.views", big.NewInt(200))
	if err != nil {
		t.Fatalf("Failed to add to counter: %v", err)
	}

	_, err = plCounter.Add(context.Background(), "count.pl.hits", big.NewInt(150))
	if err != nil {
		t.Fatalf("Failed to add to counter: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Check eu aggregation
	euHits, err := euCounter.Get(context.Background(), "count.eu.hits")
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}
	if euHits.Value.Cmp(big.NewInt(250)) != 0 {
		t.Fatalf("Expected 250, got %s", euHits.Value.String())
	}

	// TODO(pp): Is this valid? Shouldn't this be count.es.hits and count.pl.hits?
	expectedEUHitsSources := map[string]map[string]*big.Int{
		"COUNT_ES": {"count.es.hits": big.NewInt(100)},
		"COUNT_PL": {"count.pl.hits": big.NewInt(150)},
	}

	expectedEUViewsSources := map[string]map[string]*big.Int{
		"COUNT_ES": {"count.es.views": big.NewInt(200)},
	}

	var i int
	var expectedCount int64
	var expectedSources map[string]map[string]*big.Int
	entries := euCounter.GetMultiple(context.Background(), []string{"count.eu.>"})
	for entry, err := range entries {
		i++
		if err != nil {
			t.Fatalf("Failed to get entry: %v", err)
		}
		switch entry.Subject {
		case "count.eu.hits":
			expectedCount = 250
			expectedSources = expectedEUHitsSources
		case "count.eu.views":
			expectedCount = 200
			expectedSources = expectedEUViewsSources
		default:
			t.Fatalf("Unexpected subject %s in EU entries", entry.Subject)
		}

		if entry.Value.Cmp(big.NewInt(expectedCount)) != 0 {
			t.Fatalf("Expected %d for %s, got %s", expectedCount, entry.Subject, entry.Value.String())
		}
		if len(entry.Sources) != len(expectedSources) {
			t.Fatalf("Expected %d sources for %s, got %d", len(expectedSources), entry.Subject, len(entry.Sources))
		}
		for sourceStream, sourceInfo := range expectedSources {
			if sources, ok := entry.Sources[sourceStream]; !ok {
				t.Fatalf("Expected source %s not found in entry sources", sourceStream)
			} else {
				for key, value := range sourceInfo {
					if sources[key].Cmp(value) != 0 {
						t.Fatalf("Expected source %s[%s] = %s, got %s", sourceStream, key, value, sources[key])
					}
				}
			}
		}
	}
	if i != 2 {
		t.Fatalf("Expected 2 entries in counter, got %d", i)
	}

	// Check global level aggregation (should have transformed subjects again)
	globalHits, err := globalCounter.Get(context.Background(), "count.hits")
	if err != nil {
		t.Fatalf("Failed to load global hits: %v", err)
	}
	if globalHits.Value.Cmp(big.NewInt(250)) != 0 {
		t.Fatalf("Expected global hits 150, got %s", globalHits.Value.String())
	}

	globalViews, err := globalCounter.Load(context.Background(), "count.views")
	if err != nil {
		t.Fatalf("Failed to load global views: %v", err)
	}
	if globalViews.Cmp(big.NewInt(200)) != 0 {
		t.Fatalf("Expected global views 200, got %s", globalViews.String())
	}

	expectedSources = map[string]map[string]*big.Int{
		"COUNT_EU": {"count.eu.hits": big.NewInt(250)},
	}
	if len(globalHits.Sources) != len(expectedSources) {
		t.Fatalf("Expected %d sources for global hits, got %d", len(expectedSources), len(globalHits.Sources))
	}
	for sourceStream, sourceInfo := range expectedSources {
		if sources, ok := globalHits.Sources[sourceStream]; !ok {
			t.Fatalf("Expected source %s not found in global hits sources", sourceStream)
		} else {
			for key, value := range sourceInfo {
				if sources[key].Cmp(value) != 0 {
					t.Fatalf("Expected global hits source %s[%s] = %s, got %s", sourceStream, key, value, sources[key])
				}
			}
		}
	}
}

func RunBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return natsserver.RunServer(&opts)
}

func shutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
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
