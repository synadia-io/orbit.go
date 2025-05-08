package streamconsumergroup

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"reflect"
	"testing"
	"time"
)

func requireNoValidationError(t testing.TB, testConfig ElasticConsumerGroupConfig) {
	t.Helper()
	if err := validateConfig(testConfig); err != nil {
		t.Fatalf("%+v should be valid: %+v", testConfig, err)
	}
}

func requireValidationError(t testing.TB, testConfig ElasticConsumerGroupConfig) {
	t.Helper()
	if err := validateConfig(testConfig); err == nil {
		t.Fatalf("%+v should not be valid", testConfig)
	}
}

func TestBaseFunctions(t *testing.T) {

	partitions := uint(4)

	testingC1 := ElasticConsumerGroupConfig{
		MaxMembers:            partitions,
		Members:               []string{"m1", "m2", "m3"},
		Filter:                "foo.*.*.>",
		PartitioningWildcards: []int{1, 2},
	}
	dest := getPartitioningTransformDest(testingC1)

	if dest != fmt.Sprintf("{{Partition(%d,1,2)}}.foo.{{Wildcard(1)}}.{{Wildcard(2)}}.>", partitions) {
		t.Fatalf("Expected dest to be {{Partition(%d,1,2)}}.foo.{{Wildcard(1)}}.{{Wildcard(2)}}.>, got %s", partitions, dest)
	}

	testingC1.MaxMembers = 6

	if !reflect.DeepEqual(ElasticGetPartitionFilters(testingC1, "m1"), []string{"0.>", "1.>"}) {
		t.Fatalf("Expected member 1 to have filters \"0.>\", \"1.>\" got %s", ElasticGetPartitionFilters(testingC1, "m1"))
	}
	if !reflect.DeepEqual(ElasticGetPartitionFilters(testingC1, "m2"), []string{"2.>", "3.>"}) {
		t.Fatalf("Expected member 1 to have filters \"2.>\", \"3.>\" got %s", ElasticGetPartitionFilters(testingC1, "m2"))
	}
	if !reflect.DeepEqual(ElasticGetPartitionFilters(testingC1, "m3"), []string{"4.>", "5.>"}) {
		t.Fatalf("Expected member 1 to have filters \"4.>\", \"5.>\" got %s", ElasticGetPartitionFilters(testingC1, "m3"))
	}

	testingC1.MaxMembers = 7

	if !reflect.DeepEqual(ElasticGetPartitionFilters(testingC1, "m1"), []string{"0.>", "1.>", "6.>"}) {
		t.Fatalf("Expected member 1 to have filters \"0.>\", \"1.>\", \"6.>\" got %s", ElasticGetPartitionFilters(testingC1, "m1"))
	}
	if !reflect.DeepEqual(ElasticGetPartitionFilters(testingC1, "m2"), []string{"2.>", "3.>"}) {
		t.Fatalf("Expected member 1 to have filters \"2.>\", \"3.>\" got %s", ElasticGetPartitionFilters(testingC1, "m2"))
	}
	if !reflect.DeepEqual(ElasticGetPartitionFilters(testingC1, "m3"), []string{"4.>", "5.>"}) {
		t.Fatalf("Expected member 1 to have filters \"4.>\", \"5.>\" got %s", ElasticGetPartitionFilters(testingC1, "m3"))
	}

	testingC1.MaxMembers = 8

	if !reflect.DeepEqual(ElasticGetPartitionFilters(testingC1, "m1"), []string{"0.>", "1.>", "6.>"}) {
		t.Fatalf("Expected member 1 to have filters \"0.>\", \"1.>\", \"6.>\" got %s", ElasticGetPartitionFilters(testingC1, "m1"))
	}
	if !reflect.DeepEqual(ElasticGetPartitionFilters(testingC1, "m2"), []string{"2.>", "3.>", "7.>"}) {
		t.Fatalf("Expected member 1 to have filters \"2.>\", \"3.>\", \"7.>\" got %s", ElasticGetPartitionFilters(testingC1, "m2"))
	}
	if !reflect.DeepEqual(ElasticGetPartitionFilters(testingC1, "m3"), []string{"4.>", "5.>"}) {
		t.Fatalf("Expected member 1 to have filters \"4.>\", \"5.>\" got %s", ElasticGetPartitionFilters(testingC1, "m3"))
	}

	testConfig := ElasticConsumerGroupConfig{
		MaxMembers:            2,
		Filter:                "foo.*",
		PartitioningWildcards: []int{1},
	}

	testConfig.Members = []string{"m1", "m2"}

	requireNoValidationError(t, testConfig)

	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{0, 1}}}
	requireValidationError(t, testConfig) // the Members field is still set
	testConfig.Members = nil
	requireNoValidationError(t, testConfig)
	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{1, 1}}} // duplicate partition
	requireValidationError(t, testConfig)
	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{1}}} // not enough partitions
	requireValidationError(t, testConfig)
	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{0, 1, 2}}} // too many partitions
	requireValidationError(t, testConfig)
	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{0, 2}}} // partition out of range
	requireValidationError(t, testConfig)
	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{0, 1}}, {Member: "m1", Partitions: []int{0, 1}}} // duplicate member
	requireValidationError(t, testConfig)
	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{0, 1}}, {Member: "m2", Partitions: []int{0, 1}}} // partition overlap
	requireValidationError(t, testConfig)
	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{0, 1}}, {Member: "m2", Partitions: []int{2, 3}}} // out of range
	requireValidationError(t, testConfig)
	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{0}}, {Member: "m2", Partitions: []int{1}}}
	requireNoValidationError(t, testConfig)
	testConfig.MaxMembers = 3
	requireValidationError(t, testConfig)
	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{0, 2}}, {Member: "m2", Partitions: []int{1, 2}}} // partial overlap
	requireValidationError(t, testConfig)
	testConfig.MemberMappings = []MemberMapping{{Member: "m1", Partitions: []int{0, 2}}, {Member: "m2", Partitions: []int{1}}} // all is good again
	requireNoValidationError(t, testConfig)
}

func TestStatic(t *testing.T) {
	streamName := "test"
	cgName := "group"
	var c1, c2 int

	server := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, server)

	nc, err := nats.Connect(server.ClientURL())
	require_NoError(t, err)

	ctx := context.Background()
	js, err := jetstream.New(nc)
	require_NoError(t, err)

	// Create a stream

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"bar.*"},
		SubjectTransform: &jetstream.SubjectTransformConfig{
			Source:      "bar.*",
			Destination: "{{partition(2,1)}}.bar.{{wildcard(1)}}",
		},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("bar.%d", i), []byte("payload"))
		require_NoError(t, err)
	}

	config := jetstream.ConsumerConfig{
		MaxAckPending: 1,
		AckWait:       1 * time.Second,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	_, err = CreateStatic(ctx, nc, streamName, cgName, 2, "bar.*", []string{"m1", "m2"}, []MemberMapping{})
	require_NoError(t, err)

	sc1 := func() {
		StaticConsume(ctx, nc, streamName, cgName, "m1", func(msg jetstream.Msg) {
			c1++
			msg.Ack()
		}, config)
	}

	sc2 := func() {
		StaticConsume(ctx, nc, streamName, cgName, "m2", func(msg jetstream.Msg) {
			c2++
			msg.Ack()
		}, config)
	}

	go sc1()
	go sc2()

	now := time.Now()
	for {
		if c1+c2 == 10 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Since(now) > 5*time.Second {
			t.Fatalf("timeout")
		}
	}

	err = DeleteStatic(ctx, nc, streamName, cgName)
	require_NoError(t, err)
}

func TestElastic(t *testing.T) {
	var streamName = "test"
	var cgName = "group"
	var c1, c2 int

	server := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, server)

	nc, err := nats.Connect(server.ClientURL())
	require_NoError(t, err)

	ctx := context.Background()
	js, err := jetstream.New(nc)
	require_NoError(t, err)

	// Create a stream

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"bar.*"},
	})
	require_NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("bar.%d", i), []byte("payload"))
		require_NoError(t, err)
	}

	config := jetstream.ConsumerConfig{
		MaxAckPending: 1,
		AckWait:       1 * time.Second,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}

	_, err = CreateElastic(ctx, nc, streamName, cgName, 2, "bar.*", []int{1}, -1, -1)
	require_NoError(t, err)

	ec1 := func() {
		ElasticConsume(ctx, nc, streamName, cgName, "m1", func(msg jetstream.Msg) {
			c1++
			msg.Ack()
		}, config)
	}

	ec2 := func() {
		ElasticConsume(ctx, nc, streamName, cgName, "m2", func(msg jetstream.Msg) {
			c2++
			msg.Ack()
		}, config)
	}

	go ec1()
	go ec2()

	_, err = AddMembers(ctx, nc, streamName, cgName, []string{"m1"})
	require_NoError(t, err)

	now := time.Now()
	for {
		if c1 == 10 && c2 == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Since(now) > 5*time.Second {
			t.Fatalf("timeout")
		}
	}
	require_Equal(t, c1 == 10 && c2 == 0, true)

	_, err = AddMembers(ctx, nc, streamName, cgName, []string{"m2"})
	require_NoError(t, err)

	// wait a little bit for m2 to be effectively added (deletion and re-creation of the consumers)
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("bar.%d", i), []byte("payload"))
		require_NoError(t, err)
	}

	now = time.Now()
	for {
		if c1 == 15 && c2 == 5 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Since(now) > 10*time.Second {
			println(c1, c2)
			t.Fatalf("timeout")
		}
	}

	_, err = DeleteMembers(ctx, nc, streamName, cgName, []string{"m1"})
	require_NoError(t, err)

	// wait a little bit for m1 to be effectively deleted (deletion and re-creation of the consumers)
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 10; i++ {
		_, err = js.Publish(ctx, fmt.Sprintf("bar.%d", i), []byte("payload"))
		require_NoError(t, err)
	}

	now = time.Now()
	for {
		if c1 == 15 && c2 == 15 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Since(now) > 10*time.Second {
			println(c1, c2)
			t.Fatalf("timeout")
		}
	}

	err = DeleteElastic(ctx, nc, streamName, cgName)
	require_NoError(t, err)
}
