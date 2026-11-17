// Copyright 2026 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chaos

import (
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// superCluster is a super-cluster of two clusters of three servers.
var superCluster = []api.ManagedServer{
	{Name: "c1-n1", Cluster: "c1"},
	{Name: "c1-n2", Cluster: "c1"},
	{Name: "c1-n3", Cluster: "c1"},
	{Name: "c2-n1", Cluster: "c2"},
	{Name: "c2-n2", Cluster: "c2"},
	{Name: "c2-n3", Cluster: "c2"},
}

func names(servers []api.ManagedServer) []string {
	var out []string
	for _, s := range servers {
		out = append(out, s.Name)
	}
	return out
}

func TestNodeSelector(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))

	cases := []struct {
		name string
		sel  NodeSelector
		want int
		from []string
	}{
		{"every server", NodeSelector{}, 6, names(superCluster)},
		{"by name", NodeSelector{Names: []string{"c1-n2", "c2-n3", "missing"}}, 2, []string{"c1-n2", "c2-n3"}},
		{"by cluster", NodeSelector{Cluster: "c2"}, 3, []string{"c2-n1", "c2-n2", "c2-n3"}},
		{"count", NodeSelector{Count: 2}, 2, names(superCluster)},
		{"count of a cluster", NodeSelector{Cluster: "c1", Count: 1}, 1, []string{"c1-n1", "c1-n2", "c1-n3"}},
		{"count above the matches", NodeSelector{Cluster: "c1", Count: 10}, 3, []string{"c1-n1", "c1-n2", "c1-n3"}},
		{"fraction rounds up", NodeSelector{Fraction: 0.1}, 1, names(superCluster)},
		{"half", NodeSelector{Fraction: 0.5}, 3, names(superCluster)},
		{"whole", NodeSelector{Fraction: 1}, 6, names(superCluster)},
		{"no match", NodeSelector{Cluster: "c3", Count: 1}, 0, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.sel.Select(superCluster, rng)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tc.want {
				t.Fatalf("got %v, want %d servers", names(got), tc.want)
			}
			for _, s := range got {
				if !slices.Contains(tc.from, s.Name) {
					t.Fatalf("picked %s, not one of %v", s.Name, tc.from)
				}
			}
			if !slices.IsSortedFunc(got, func(a, b api.ManagedServer) int {
				return slices.Index(names(superCluster), a.Name) - slices.Index(names(superCluster), b.Name)
			}) {
				t.Fatalf("picked servers %v are not in instance order", names(got))
			}
		})
	}

	invalid := []NodeSelector{
		{Count: -1},
		{Fraction: -0.5},
		{Fraction: 1.5},
		{Count: 1, Fraction: 0.5},
		{Names: []string{"c1-n1", "c2-n1"}, Cluster: "c2"},
	}
	for _, sel := range invalid {
		_, err := sel.Select(superCluster, rng)
		if !errors.Is(err, ErrInvalidSelector) {
			t.Errorf("%+v: got %v, want ErrInvalidSelector", sel, err)
		}
	}
}

func TestPickFraction(t *testing.T) {
	cases := []struct {
		items    int
		fraction float64
		want     int
	}{
		{100, 0.07, 7},
		{100, 0.14, 14},
		{100, 0.29, 29},
		{100, 0.57, 57},
		{10, 0.7, 7},
		{3, 0.1, 1},
		{100, 0.001, 1},
		{1, 1e-300, 1},
		{100, 0.071, 8},
		{6, 0.5, 3},
		{5, 0.5, 3},
		{1000, 0.999, 999},
		{1000, 0.9991, 1000},
		{10, 1, 10},
		{0, 0.5, 0},
	}

	for _, tc := range cases {
		items := make([]int, tc.items)
		got := Pick(items, 0, tc.fraction, nil)
		if len(got) != tc.want {
			t.Errorf("%v of %d: picked %d, want %d", tc.fraction, tc.items, len(got), tc.want)
		}
	}
}

func TestPickIsRandom(t *testing.T) {
	seen := map[string]bool{}
	for range 200 {
		got := Pick(superCluster, 1, 0, nil)
		seen[got[0].Name] = true
	}
	if len(seen) < 2 {
		t.Fatalf("200 picks of one server all picked %v", seen)
	}

	original := names(superCluster)
	Pick(superCluster, 3, 0, nil)
	if !slices.Equal(original, names(superCluster)) {
		t.Fatal("Pick changed its input")
	}
}

func TestStreamSelector(t *testing.T) {
	sel := StreamSelector{Account: "USERS1", Name: "ORDERS*"}

	err := sel.Validate()
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		account string
		name    string
		want    bool
	}{
		{"USERS1", "ORDERS", true},
		{"USERS1", "ORDERS_EU", true},
		{"USERS1", "INVOICES", false},
		{"USERS2", "ORDERS", false},
	}
	for _, tc := range cases {
		got := sel.Match(tc.account, tc.name)
		if got != tc.want {
			t.Errorf("match %s %s: got %v, want %v", tc.account, tc.name, got, tc.want)
		}
	}

	every := StreamSelector{Account: "USERS1"}
	if !every.Match("USERS1", "ANYTHING") {
		t.Error("an empty name pattern does not match every stream")
	}

	for _, bad := range []StreamSelector{
		{Name: "X"},
		{Account: "A", Name: "[x"},
		{Account: "A", Count: -1},
		{Account: "A", Fraction: -0.5},
		{Account: "A", Fraction: 1.5},
		{Account: "A", Count: 1, Fraction: 0.5},
	} {
		err = bad.Validate()
		if !errors.Is(err, ErrInvalidSelector) {
			t.Errorf("%+v: got %v, want ErrInvalidSelector", bad, err)
		}
	}
}

// TestStreamAndConsumerSelectorAmount checks stream and consumer selectors
// pick one match when they set neither count nor fraction, unlike node and
// client selectors, and Count or Fraction otherwise, rounding a fraction up.
func TestStreamAndConsumerSelectorAmount(t *testing.T) {
	streams := []string{"S1", "S2", "S3", "S4", "S5"}

	cases := []struct {
		name     string
		count    int
		fraction float64
		want     int
	}{
		{"neither picks one", 0, 0, 1},
		{"count", 2, 0, 2},
		{"count above the matches", 10, 0, 5},
		{"fraction rounds up", 0, 0.5, 3},
		{"whole", 0, 1, 5},
		{"small fraction picks one", 0, 0.01, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ss := StreamSelector{Account: "A", Count: tc.count, Fraction: tc.fraction}
			err := ss.Validate()
			if err != nil {
				t.Fatal(err)
			}
			count, fraction := ss.Amount()
			got := Pick(streams, count, fraction, nil)
			if len(got) != tc.want {
				t.Errorf("streams: picked %v, want %d", got, tc.want)
			}

			cs := ConsumerSelector{Account: "A", Count: tc.count, Fraction: tc.fraction}
			err = cs.Validate()
			if err != nil {
				t.Fatal(err)
			}
			count, fraction = cs.Amount()
			got = Pick(streams, count, fraction, nil)
			if len(got) != tc.want {
				t.Errorf("consumers: picked %v, want %d", got, tc.want)
			}
		})
	}

	all := Pick(streams, 0, 0, nil)
	if len(all) != len(streams) {
		t.Fatalf("Pick with neither set picked %v, want every item as node selectors do", all)
	}
	nodes, err := (NodeSelector{}).Select(superCluster, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != len(superCluster) {
		t.Fatalf("a node selector with neither set picked %v, want every server", names(nodes))
	}
}

func TestConsumerSelector(t *testing.T) {
	sel := ConsumerSelector{Account: "USERS1", Stream: "ORDERS", Name: "worker-?"}

	err := sel.Validate()
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		account  string
		stream   string
		consumer string
		want     bool
	}{
		{"USERS1", "ORDERS", "worker-1", true},
		{"USERS1", "ORDERS", "worker-10", false},
		{"USERS1", "INVOICES", "worker-1", false},
		{"USERS2", "ORDERS", "worker-1", false},
	}
	for _, tc := range cases {
		got := sel.Match(tc.account, tc.stream, tc.consumer)
		if got != tc.want {
			t.Errorf("match %s %s %s: got %v, want %v", tc.account, tc.stream, tc.consumer, got, tc.want)
		}
	}

	for _, bad := range []ConsumerSelector{
		{Stream: "X"},
		{Account: "A", Stream: "[x"},
		{Account: "A", Name: "[x"},
		{Account: "A", Count: -1},
		{Account: "A", Fraction: 2},
		{Account: "A", Count: 1, Fraction: 0.5},
	} {
		err = bad.Validate()
		if !errors.Is(err, ErrInvalidSelector) {
			t.Errorf("%+v: got %v, want ErrInvalidSelector", bad, err)
		}
	}
}

func TestClientSelector(t *testing.T) {
	sel := ClientSelector{Account: "USERS1", Name: "bench-*", Fraction: 0.5}

	err := sel.Validate()
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		account string
		name    string
		want    bool
	}{
		{"USERS1", "bench-1", true},
		{"USERS1", "other", false},
		{"USERS2", "bench-1", false},
	}
	for _, tc := range cases {
		got := sel.Match(tc.account, tc.name)
		if got != tc.want {
			t.Errorf("match %s %s: got %v, want %v", tc.account, tc.name, got, tc.want)
		}
	}

	if !(ClientSelector{}).Match("USERS3", "") {
		t.Error("an empty selector does not match every client")
	}

	clients := []int{1, 2, 3, 4, 5}
	picked := Pick(clients, sel.Count, sel.Fraction, nil)
	if len(picked) != 3 {
		t.Errorf("half of 5 clients: got %v, want 3", picked)
	}

	for _, bad := range []ClientSelector{{Count: 1, Fraction: 0.1}, {Name: "[x"}, {Fraction: 2}} {
		err = bad.Validate()
		if !errors.Is(err, ErrInvalidSelector) {
			t.Errorf("%+v: got %v, want ErrInvalidSelector", bad, err)
		}
	}
}

func TestLinkSelector(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))

	t.Run("by nodes", func(t *testing.T) {
		sel := LinkSelector{Kind: api.ToxicKindRoute, Nodes: &NodeSelector{Cluster: "c1", Count: 2}}
		got, err := sel.Resolve(superCluster, rng)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("got %+v, want 2 selectors", got)
		}
		for _, ts := range got {
			if ts.Kind != api.ToxicKindRoute || ts.From != "" || ts.To != "" || ts.Direction != "" {
				t.Errorf("selector %+v is not a route selector of one node", ts)
			}
			if !slices.Contains([]string{"c1-n1", "c1-n2", "c1-n3"}, ts.Node) {
				t.Errorf("selector names node %q, not one of c1", ts.Node)
			}
		}
		if got[0].Node == got[1].Node {
			t.Errorf("both selectors name %s", got[0].Node)
		}
	})

	t.Run("by nodes with no match", func(t *testing.T) {
		got, err := LinkSelector{Nodes: &NodeSelector{Cluster: "c3"}}.Resolve(superCluster, rng)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("got %+v, want none", got)
		}
	})

	t.Run("by ends", func(t *testing.T) {
		sel := LinkSelector{Kind: api.ToxicKindGateway, From: "c1", To: "c2", Direction: api.ToxicDirectionTo}
		got, err := sel.Resolve(superCluster, rng)
		if err != nil {
			t.Fatal(err)
		}
		want := []api.ToxicSelector{{Kind: api.ToxicKindGateway, From: "c1", To: "c2", Direction: api.ToxicDirectionTo}}
		if !slices.Equal(got, want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})

	t.Run("every link", func(t *testing.T) {
		got, err := LinkSelector{}.Resolve(superCluster, rng)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, []api.ToxicSelector{{}}) {
			t.Fatalf("got %+v, want one empty selector", got)
		}
	})

	valid := []LinkSelector{
		{Direction: api.ToxicDirectionTo, To: "c2"},
		{Direction: api.ToxicDirectionFrom, From: "c1"},
		{Direction: api.ToxicDirectionBoth},
		{Kind: api.ToxicKindClient, To: "c1-n1", Direction: api.ToxicDirectionTo},
		{Kind: api.ToxicKindClient, Nodes: &NodeSelector{Count: 1}},
	}
	for _, sel := range valid {
		err := sel.Validate()
		if err != nil {
			t.Errorf("%+v: %v", sel, err)
		}
	}

	invalid := []LinkSelector{
		{Kind: "leaf"},
		{Direction: "sideways"},
		{Nodes: &NodeSelector{Count: 1}, From: "c1-n1"},
		{Nodes: &NodeSelector{Count: 1}, To: "c1-n1"},
		{Nodes: &NodeSelector{Count: 1}, Direction: api.ToxicDirectionTo},
		{Nodes: &NodeSelector{Count: 1, Fraction: 0.5}},
		{Direction: api.ToxicDirectionTo},
		{Direction: api.ToxicDirectionFrom},
		{Kind: api.ToxicKindRoute, Direction: api.ToxicDirectionTo},
		{Kind: api.ToxicKindClient, From: "c1-n1"},
		{Kind: api.ToxicKindClient, From: "c1-n1", To: "c1-n2"},
		{Kind: api.ToxicKindClient, Nodes: &NodeSelector{Count: 1}, From: "c1-n1"},
	}
	for _, sel := range invalid {
		_, err := sel.Resolve(superCluster, rng)
		if !errors.Is(err, ErrInvalidSelector) {
			t.Errorf("%+v: got %v, want ErrInvalidSelector", sel, err)
		}
	}
}
