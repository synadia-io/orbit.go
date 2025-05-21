package partitionedconsumergroups

import (
	"fmt"
	"reflect"
	"testing"
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
