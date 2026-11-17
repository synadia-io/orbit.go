package ntf

import (
	"fmt"
	"slices"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// The kinds of instance a create makes, as the service reports them on
// Instance.Kind.
const (
	kindServer       = "server"
	kindCluster      = "cluster"
	kindSuperCluster = "super-cluster"
)

// kindOpt records a create option that applies to one kind of instance only, so
// the create can refuse it on any other kind.
type kindOpt struct {
	option string
	kind   string
}

// WithName names the server of a CreateServer call. A name starts with a
// letter, holds only letters, digits, _ and -, and is at most 32 characters;
// the service refuses any other. Without it the server is named n1 (or
// <short>-n1, after the instance's short ID, by services from before default
// names dropped it).
//
// Server names are unique within an instance but not across instances; see
// Manager.StopInstanceServer for operating on a name that more than one
// instance holds.
//
// It applies to CreateServer only: any other create returns
// ErrInvalidCreateOption. A later WithName replaces an earlier one.
func WithName(name string) CreateOption {
	return createOpt(func(o *createOptions) {
		o.name = name
		o.kindOpts = append(o.kindOpts, kindOpt{option: "WithName", kind: kindServer})
	})
}

// WithClusterName names the cluster of a CreateCluster call. It follows the
// name rule of WithName and is at most 28 characters. Without it the cluster is
// named c1 and its servers n<i> (C_<short> and <short>-n<i> by services from
// before default names dropped the short ID). A server of a named cluster that
// WithServerNames does not name is <cluster>-n<i>.
//
// It applies to CreateCluster only: any other create returns
// ErrInvalidCreateOption; name the clusters of a super-cluster with
// WithClusters. A later WithClusterName replaces an earlier one.
func WithClusterName(name string) CreateOption {
	return createOpt(func(o *createOptions) {
		o.clusterName = name
		o.kindOpts = append(o.kindOpts, kindOpt{option: "WithClusterName", kind: kindCluster})
	})
}

// WithServerNames names the servers of a CreateCluster call, in order, one name
// per server; each follows the name rule of WithName. The servers count may
// then be zero, and the cluster has one server per name; a non-zero count must
// equal the number of names, or the service refuses the create.
//
// It applies to CreateCluster only: any other create returns
// ErrInvalidCreateOption; name the servers of a super-cluster with
// WithClusters. Without names it returns ErrInvalidCreateOption too. A later
// WithServerNames replaces an earlier one. The names are copied, so changing
// the caller's slice afterwards does not change the option.
func WithServerNames(names ...string) CreateOption {
	given := slices.Clone(names)
	if given == nil {
		given = []string{}
	}

	return createOpt(func(o *createOptions) {
		o.serverNames = given
		o.kindOpts = append(o.kindOpts, kindOpt{option: "WithServerNames", kind: kindCluster})
	})
}

// WithClusters lists the clusters of a CreateSuperCluster call in order, each
// with its own name, size and server names, in place of the clusters and
// servers counts. The service takes 2 to 10 clusters of 1 to 20 servers each.
// Build the entries with SizedCluster and ClusterWithServerNames, or as
// api.ClusterSpec values. The entries are copied, so changing them or the
// caller's slice afterwards does not change the option.
//
// With it, the clusters and servers arguments of CreateSuperCluster (and of the
// Client super-cluster helpers) must both be zero, or the create returns
// ErrInvalidCreateOption; the request carries the list alone.
//
// An entry without a name keeps the default name of its position, c<c>, and
// the unnamed servers of any cluster are <cluster>-n<i>. Services from before
// default names dropped the short ID name an unnamed entry SC_<short>_<c> and
// its unnamed servers <short>-c<c>_s<i>.
//
// It applies to CreateSuperCluster only: any other create returns
// ErrInvalidCreateOption. Without entries it returns ErrInvalidCreateOption too.
// A later WithClusters replaces an earlier one.
func WithClusters(clusters ...api.ClusterSpec) CreateOption {
	given := make([]api.ClusterSpec, 0, len(clusters))
	for _, c := range clusters {
		c.ServerNames = slices.Clone(c.ServerNames)
		given = append(given, c)
	}

	return createOpt(func(o *createOptions) {
		o.clusterList = given
		o.kindOpts = append(o.kindOpts, kindOpt{option: "WithClusters", kind: kindSuperCluster})
	})
}

// SizedCluster returns a WithClusters entry for a cluster of servers servers
// named name, its servers named <name>-n<i>. An empty name keeps the default
// name of the cluster's position, and the default server names with it.
func SizedCluster(name string, servers int) api.ClusterSpec {
	return api.ClusterSpec{Name: name, Servers: servers}
}

// ClusterWithServerNames returns a WithClusters entry for a cluster named name
// with one server per entry of serverNames, named so in order. An empty name
// keeps the default name of the cluster's position.
func ClusterWithServerNames(name string, serverNames ...string) api.ClusterSpec {
	return api.ClusterSpec{Name: name, ServerNames: slices.Clone(serverNames)}
}

// checkKind returns ErrInvalidCreateOption naming the first option given that
// does not apply to a create of kind.
func (o createOptions) checkKind(kind string) error {
	for _, k := range o.kindOpts {
		if k.kind != kind {
			return fmt.Errorf("%w: %s applies to a %s create, not a %s create", ErrInvalidCreateOption, k.option, k.kind, kind)
		}
	}
	return nil
}

// clusterSize is the number of servers a CreateCluster of servers servers with
// these options makes: the number of WithServerNames names when servers is
// zero.
func (o createOptions) clusterSize(servers int) int {
	if servers == 0 && o.serverNames != nil {
		return len(o.serverNames)
	}
	return servers
}

// superClusterSize is the number of servers a CreateSuperCluster of clusters
// clusters of servers servers with these options makes: the sum of the
// WithClusters entries' sizes when the list is given.
func (o createOptions) superClusterSize(clusters int, servers int) int {
	if o.clusterList == nil {
		return clusters * servers
	}

	n := 0
	for _, c := range o.clusterList {
		if c.Servers == 0 {
			n += len(c.ServerNames)
			continue
		}
		n += c.Servers
	}
	return n
}
