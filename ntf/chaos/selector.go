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
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"path"
	"slices"
	"strconv"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// Selectors are the shared types a kind's params use to choose its targets.
// A kind matches them afresh on every Apply, so a run follows servers, streams
// and clients that change while it runs. Name patterns are shell globs as
// path.Match reads them, such as "ORDERS*", and an empty pattern matches any
// name.

// NodeSelector picks servers of an instance: those named in Names, or those of
// the cluster Cluster, or every one when both are empty, and then Count or a
// Fraction of them, at random, or every match when both are zero. Names and
// Cluster cannot both be set, since Names already picks exact servers.
type NodeSelector struct {
	// Names are server names. Empty matches any server.
	Names []string `json:"names,omitempty"`

	// Cluster is a cluster name. Empty matches any cluster.
	Cluster string `json:"cluster,omitempty"`

	// Count picks this many of the matching servers, or all of them when
	// fewer match.
	Count int `json:"count,omitempty"`

	// Fraction picks this share of the matching servers, above 0 and at most
	// 1, rounded up, so at least one is picked whenever one matches.
	Fraction float64 `json:"fraction,omitempty"`
}

// Validate checks that Names and Cluster are not both set, and that Count and
// Fraction are in range and not both set.
func (s NodeSelector) Validate() error {
	if len(s.Names) > 0 && s.Cluster != "" {
		return fmt.Errorf("%w: nodes by names cannot also name a cluster", ErrInvalidSelector)
	}

	return validateAmount(s.Count, s.Fraction)
}

// Match reports whether server matches Names and Cluster.
func (s NodeSelector) Match(server api.ManagedServer) bool {
	if len(s.Names) > 0 && !slices.Contains(s.Names, server.Name) {
		return false
	}
	if s.Cluster != "" && server.Cluster != s.Cluster {
		return false
	}

	return true
}

// Select returns the servers that match, then Count or Fraction of them
// chosen with rng, or with the global source when rng is nil. They keep the
// order of servers.
func (s NodeSelector) Select(servers []api.ManagedServer, rng *rand.Rand) ([]api.ManagedServer, error) {
	err := s.Validate()
	if err != nil {
		return nil, err
	}

	var matched []api.ManagedServer
	for _, server := range servers {
		if s.Match(server) {
			matched = append(matched, server)
		}
	}

	return Pick(matched, s.Count, s.Fraction, rng), nil
}

// StreamSelector matches streams by account and name.
type StreamSelector struct {
	// Account is the account the streams are in. It is required.
	Account string `json:"account"`

	// Name is a pattern of stream names.
	Name string `json:"name,omitempty"`
}

// Validate checks that Account is set and Name is a valid pattern.
func (s StreamSelector) Validate() error {
	if s.Account == "" {
		return fmt.Errorf("%w: streams need an account", ErrInvalidSelector)
	}

	return validatePattern("name", s.Name)
}

// Match reports whether the stream named name in account matches.
func (s StreamSelector) Match(account string, name string) bool {
	return account == s.Account && matchPattern(s.Name, name)
}

// ConsumerSelector matches consumers by account, stream name and consumer
// name.
type ConsumerSelector struct {
	// Account is the account the consumers are in. It is required.
	Account string `json:"account"`

	// Stream is a pattern of the names of the streams the consumers are on.
	Stream string `json:"stream,omitempty"`

	// Name is a pattern of consumer names.
	Name string `json:"name,omitempty"`
}

// Validate checks that Account is set and the patterns are valid.
func (s ConsumerSelector) Validate() error {
	if s.Account == "" {
		return fmt.Errorf("%w: consumers need an account", ErrInvalidSelector)
	}
	err := validatePattern("stream", s.Stream)
	if err != nil {
		return err
	}

	return validatePattern("name", s.Name)
}

// Match reports whether the consumer named name on stream in account matches.
func (s ConsumerSelector) Match(account string, stream string, name string) bool {
	return account == s.Account && matchPattern(s.Stream, stream) && matchPattern(s.Name, name)
}

// ClientSelector matches client connections by account and connection name,
// and picks Count or a Fraction of the matches, at random, or every match when
// both are zero. A kind picks with Pick.
type ClientSelector struct {
	// Account is the account the clients are in. Empty matches any account.
	Account string `json:"account,omitempty"`

	// Name is a pattern of connection names.
	Name string `json:"name,omitempty"`

	// Count picks this many of the matching clients, or all of them when
	// fewer match.
	Count int `json:"count,omitempty"`

	// Fraction picks this share of the matching clients, above 0 and at most
	// 1, rounded up, so at least one is picked whenever one matches.
	Fraction float64 `json:"fraction,omitempty"`
}

// Validate checks the pattern, and that Count and Fraction are in range and
// not both set.
func (s ClientSelector) Validate() error {
	err := validatePattern("name", s.Name)
	if err != nil {
		return err
	}

	return validateAmount(s.Count, s.Fraction)
}

// Match reports whether a client in account with connection name name
// matches.
func (s ClientSelector) Match(account string, name string) bool {
	if s.Account != "" && account != s.Account {
		return false
	}

	return matchPattern(s.Name, name)
}

// LinkSelector picks links of an instance created with toxics, and resolves
// to the api.ToxicSelector values a toxic is added with. With Nodes it picks
// servers and resolves to one selector per server, whose Node names it, so a
// toxic acts on every link of that server. Without Nodes it resolves to one
// selector of Kind, From, To and Direction.
type LinkSelector struct {
	// Kind is one of the api.ToxicKind* constants. Empty selects every kind.
	Kind string `json:"kind,omitempty"`

	// Nodes picks the servers whose links are selected. It cannot be combined
	// with From or To, or with a Direction other than both.
	Nodes *NodeSelector `json:"nodes,omitempty"`

	// From is a node or cluster at one end of the links, as
	// api.ToxicSelector.From.
	From string `json:"from,omitempty"`

	// To is a node or cluster at the other end of the links, as
	// api.ToxicSelector.To.
	To string `json:"to,omitempty"`

	// Direction is one of the api.ToxicDirection* constants. Empty means
	// both.
	Direction string `json:"direction,omitempty"`
}

// Validate checks Kind, Direction and Nodes, that Nodes is not combined with
// From, To or a one-way Direction, that a one-way Direction has From or To to
// say which way the data flows, and that client links have no From.
func (s LinkSelector) Validate() error {
	switch s.Kind {
	case "", api.ToxicKindRoute, api.ToxicKindGateway, api.ToxicKindClient:
	default:
		return fmt.Errorf("%w: unknown link kind %q", ErrInvalidSelector, s.Kind)
	}

	switch s.Direction {
	case "", api.ToxicDirectionBoth, api.ToxicDirectionTo, api.ToxicDirectionFrom:
	default:
		return fmt.Errorf("%w: unknown link direction %q", ErrInvalidSelector, s.Direction)
	}

	if s.Kind == api.ToxicKindClient && s.From != "" {
		return fmt.Errorf("%w: a client link has no from, name its server with to or nodes", ErrInvalidSelector)
	}

	oneWay := s.Direction != "" && s.Direction != api.ToxicDirectionBoth

	if s.Nodes == nil {
		if oneWay && s.From == "" && s.To == "" {
			return fmt.Errorf("%w: direction %q needs from or to to say which way the data flows", ErrInvalidSelector, s.Direction)
		}

		return nil
	}
	if s.From != "" || s.To != "" {
		return fmt.Errorf("%w: links by nodes cannot also name from or to", ErrInvalidSelector)
	}
	if oneWay {
		return fmt.Errorf("%w: links by nodes act both ways, not %q", ErrInvalidSelector, s.Direction)
	}

	return s.Nodes.Validate()
}

// Resolve returns the toxic selectors s stands for on an instance of servers,
// picking nodes with rng, or with the global source when rng is nil. It
// returns none when Nodes matches no server.
func (s LinkSelector) Resolve(servers []api.ManagedServer, rng *rand.Rand) ([]api.ToxicSelector, error) {
	err := s.Validate()
	if err != nil {
		return nil, err
	}

	if s.Nodes == nil {
		return []api.ToxicSelector{{Kind: s.Kind, From: s.From, To: s.To, Direction: s.Direction}}, nil
	}

	picked, err := s.Nodes.Select(servers, rng)
	if err != nil {
		return nil, err
	}

	selectors := make([]api.ToxicSelector, 0, len(picked))
	for _, server := range picked {
		selectors = append(selectors, api.ToxicSelector{Kind: s.Kind, Node: server.Name, Direction: s.Direction})
	}

	return selectors, nil
}

// Pick returns count of items, or fraction of them rounded up, chosen with
// rng, or with the global source when rng is nil. It returns every item when
// count and fraction are both zero or ask for at least as many as there are.
// The picked items keep their order.
func Pick[T any](items []T, count int, fraction float64, rng *rand.Rand) []T {
	n := len(items)
	if fraction > 0 {
		n = fractionOf(len(items), fraction)
	} else if count > 0 {
		n = count
	}
	if n >= len(items) {
		return slices.Clone(items)
	}

	perm := rand.Perm
	if rng != nil {
		perm = rng.Perm
	}
	chosen := perm(len(items))[:n]
	slices.Sort(chosen)

	picked := make([]T, 0, n)
	for _, i := range chosen {
		picked = append(picked, items[i])
	}

	return picked
}

// fractionOf returns fraction of n rounded up, at least one when n is above
// zero, for a fraction above zero. It computes with the shortest decimal that
// reads back as fraction, so 0.07 of 100 is exactly 7 and not 8 as the float
// product 7.000000000000001 rounds up to. A fraction of 1 or more is all of n.
func fractionOf(n int, fraction float64) int {
	if fraction >= 1 {
		return n
	}

	share, ok := new(big.Rat).SetString(strconv.FormatFloat(fraction, 'g', -1, 64))
	if !ok {
		return n
	}
	share.Mul(share, new(big.Rat).SetInt64(int64(n)))

	picked, rem := new(big.Int).QuoRem(share.Num(), share.Denom(), new(big.Int))
	if rem.Sign() != 0 {
		picked.Add(picked, big.NewInt(1))
	}

	return int(picked.Int64())
}

// validateAmount checks a count and fraction pair.
func validateAmount(count int, fraction float64) error {
	if count < 0 {
		return fmt.Errorf("%w: count %d is negative", ErrInvalidSelector, count)
	}
	if math.IsNaN(fraction) || fraction < 0 || fraction > 1 {
		return fmt.Errorf("%w: fraction %v is not above 0 and at most 1", ErrInvalidSelector, fraction)
	}
	if count > 0 && fraction > 0 {
		return fmt.Errorf("%w: count and fraction cannot both be set", ErrInvalidSelector)
	}

	return nil
}

// validatePattern checks that pattern is a valid glob.
func validatePattern(field string, pattern string) error {
	_, err := path.Match(pattern, "")
	if err != nil {
		return fmt.Errorf("%w: %s pattern %q: %w", ErrInvalidSelector, field, pattern, err)
	}

	return nil
}

// matchPattern reports whether name matches the glob pattern, an empty pattern
// matching any name. An invalid pattern matches nothing.
func matchPattern(pattern string, name string) bool {
	if pattern == "" {
		return true
	}
	ok, err := path.Match(pattern, name)
	if err != nil {
		return false
	}

	return ok
}
