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

package ntf

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/synadia-io/orbit.go/ntf/api"
)

const (
	// maxNameLen caps a given server name.
	maxNameLen = 32

	// maxClusterNameLen caps a given cluster name, so the name <cluster>-n20
	// its servers get by default still fits maxNameLen.
	maxClusterNameLen = 28

	// minServersPerListedCluster is the fewest servers a cluster of a
	// super-cluster's cluster_list may have. A super-cluster of one-server
	// clusters plans, gateways and runs JetStream like any other.
	minServersPerListedCluster = 1
)

// nameRule is the rule every given name follows: a letter, then letters,
// digits, _ or -, at most maxNameLen characters in all.
var nameRule = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,` + strconv.Itoa(maxNameLen-1) + `}$`)

// checkGivenServerName returns an error when name, the name a create gives a
// server, does not follow nameRule. what says which name it is.
func checkGivenServerName(what, name string) error {
	if !nameRule.MatchString(name) {
		return fmt.Errorf("%s must start with a letter and hold only letters, digits, _ and -, at most %d characters, not %q", what, maxNameLen, name)
	}

	return nil
}

// checkGivenClusterName returns an error when name, the name a create gives a
// cluster, does not follow nameRule or is longer than maxClusterNameLen. what
// says which name it is.
func checkGivenClusterName(what, name string) error {
	if !nameRule.MatchString(name) || len(name) > maxClusterNameLen {
		return fmt.Errorf("%s must start with a letter and hold only letters, digits, _ and -, at most %d characters, not %q", what, maxClusterNameLen, name)
	}

	return nil
}

// checkGivenServerNames checks each of names, the server names a create gives
// the cluster label names, against nameRule.
func checkGivenServerNames(label string, names []string) error {
	for i, name := range names {
		err := checkGivenServerName(fmt.Sprintf("the name of server %d of %s", i+1, label), name)
		if err != nil {
			return err
		}
	}

	return nil
}

// clusterLabel names the cluster at 1-based position pos of a create, by its
// given name when it has one and by its position otherwise, for the errors
// about it.
func clusterLabel(pos int, name string) string {
	if name != "" {
		return "cluster " + name
	}

	return fmt.Sprintf("cluster %d", pos)
}

// clusterSize returns the size of a cluster a create gives as servers and
// names: servers, or the number of names when servers is left out. An error
// says the cluster label names gives neither, or a servers that disagrees with
// its names.
func clusterSize(label string, servers int, names []string) (int, error) {
	switch {
	case servers == 0 && len(names) == 0:
		return 0, fmt.Errorf("%s needs servers or server_names", label)
	case servers == 0:
		return len(names), nil
	case len(names) > 0 && len(names) != servers:
		return 0, fmt.Errorf("%s has servers %d but %d server_names", label, servers, len(names))
	default:
		return servers, nil
	}
}

// specError is a create request refused by the handler, with the code it is
// answered with. Its message is answered after an Invalid request: prefix.
type specError struct {
	Code    string
	Message string
}

// Error returns the message the request is answered with.
func (e *specError) Error() string {
	return "Invalid request: " + e.Message
}

// refuse returns a refusal with code and err's message.
func refuse(code string, err error) *specError {
	return &specError{Code: code, Message: err.Error()}
}

// clusterRequestClusters returns the cluster creq asks for, its size from
// servers or server_names and its given names, checked against the name rule
// and the size bounds, or why the request is refused.
func clusterRequestClusters(creq api.CreateClusterRequest) ([]clusterSpec, *specError) {
	size, err := clusterSize("the cluster", creq.Servers, creq.ServerNames)
	if err != nil {
		return nil, refuse("002", err)
	}

	if size < 2 {
		return nil, &specError{Code: "002", Message: "servers should be at least 2"}
	}
	if size > maxServersPerCluster {
		return nil, &specError{Code: "002", Message: fmt.Sprintf("servers should be at most %d", maxServersPerCluster)}
	}

	if creq.ClusterName != "" {
		err := checkGivenClusterName("cluster_name", creq.ClusterName)
		if err != nil {
			return nil, refuse("002", err)
		}
	}

	label := "the cluster"
	if creq.ClusterName != "" {
		label = "cluster " + creq.ClusterName
	}
	err = checkGivenServerNames(label, creq.ServerNames)
	if err != nil {
		return nil, refuse("002", err)
	}

	return []clusterSpec{{Name: creq.ClusterName, Servers: size, ServerNames: creq.ServerNames}}, nil
}

// superClusterRequestClusters returns the clusters creq asks for: from
// cluster_list when given, each with its size from servers or server_names and
// its given names, or nil for the clusters and servers counts, which it checks
// against their bounds, or why the request is refused. creq must give one form
// and not both.
func superClusterRequestClusters(creq api.CreateSuperClusterRequest) ([]clusterSpec, *specError) {
	counted := creq.Servers != 0 || creq.Clusters != 0

	switch {
	case counted && len(creq.ClusterList) > 0:
		return nil, &specError{Code: "002", Message: "give clusters and servers or cluster_list, not both"}
	case !counted && len(creq.ClusterList) == 0:
		return nil, &specError{Code: "002", Message: "give clusters and servers, or cluster_list"}
	case counted && (creq.Servers == 0 || creq.Clusters == 0):
		return nil, &specError{Code: "002", Message: "give clusters and servers together"}
	}

	if counted {
		if creq.Servers < 2 {
			return nil, &specError{Code: "002", Message: "servers should be at least 2"}
		}
		if creq.Servers > maxServersPerCluster {
			return nil, &specError{Code: "002", Message: fmt.Sprintf("servers should be at most %d", maxServersPerCluster)}
		}
		if creq.Clusters < 2 {
			return nil, &specError{Code: "004", Message: "clusters should be at least 2"}
		}
		if creq.Clusters > maxClusters {
			return nil, &specError{Code: "004", Message: fmt.Sprintf("clusters should be at most %d", maxClusters)}
		}
		return nil, nil
	}

	if len(creq.ClusterList) < 2 {
		return nil, &specError{Code: "004", Message: "cluster_list should hold at least 2 clusters"}
	}
	if len(creq.ClusterList) > maxClusters {
		return nil, &specError{Code: "004", Message: fmt.Sprintf("cluster_list should hold at most %d clusters", maxClusters)}
	}

	clusters := make([]clusterSpec, 0, len(creq.ClusterList))
	for i, c := range creq.ClusterList {
		label := clusterLabel(i+1, c.Name)

		size, err := clusterSize(label, c.Servers, c.ServerNames)
		if err != nil {
			return nil, refuse("002", err)
		}
		if size < minServersPerListedCluster {
			return nil, &specError{Code: "002", Message: fmt.Sprintf("%s should have at least %d servers", label, minServersPerListedCluster)}
		}
		if size > maxServersPerCluster {
			return nil, &specError{Code: "002", Message: fmt.Sprintf("%s should have at most %d servers", label, maxServersPerCluster)}
		}

		if c.Name != "" {
			err := checkGivenClusterName(fmt.Sprintf("the name of cluster %d", i+1), c.Name)
			if err != nil {
				return nil, refuse("002", err)
			}
		}

		err = checkGivenServerNames(label, c.ServerNames)
		if err != nil {
			return nil, refuse("002", err)
		}

		clusters = append(clusters, clusterSpec{Name: c.Name, Servers: size, ServerNames: c.ServerNames})
	}

	return clusters, nil
}

// namedCluster is a cluster of an instance with every name resolved: the
// cluster's and its servers', in order. A single server is one namedCluster
// with no name.
type namedCluster struct {
	Name    string
	Servers []string
}

// planNames returns the clusters and servers of spec with their names, the
// given ones and the default ones, and checks the whole set: every name is
// unique in the instance, so no server shares a cluster's name either, since a
// toxic selector looks a name up as a node first. No default name carries the
// instance's id, so default names repeat across instances. A server is named
// n1 by default. A cluster is named c<c> by default, after its 1-based
// position, so a cluster instance's one cluster is c1. A server of a cluster
// instance whose cluster is not given a name is n<i>, and any other server not
// given a name is <cluster>-n<i>.
func planNames(spec instanceSpec) ([]namedCluster, error) {
	if spec.Kind == "server" {
		name := spec.ServerName
		if name == "" {
			name = "n1"
		}
		return []namedCluster{{Servers: []string{name}}}, nil
	}

	specs := spec.clusters()
	clusters := make([]namedCluster, len(specs))
	for c, cs := range specs {
		cluster := &clusters[c]

		cluster.Name = cs.Name
		if cluster.Name == "" {
			cluster.Name = fmt.Sprintf("c%d", c+1)
		}

		for i := range cs.Servers {
			var name string
			switch {
			case len(cs.ServerNames) > 0:
				name = cs.ServerNames[i]
			case spec.Kind == "cluster" && cs.Name == "":
				name = fmt.Sprintf("n%d", i+1)
			default:
				name = fmt.Sprintf("%s-n%d", cluster.Name, i+1)
			}
			cluster.Servers = append(cluster.Servers, name)
		}
	}

	holders := map[string]string{}
	hold := func(name, holder string) error {
		other, taken := holders[name]
		if taken {
			return fmt.Errorf("names must be unique, %q names both %s and %s", name, other, holder)
		}
		holders[name] = holder
		return nil
	}

	for _, cluster := range clusters {
		err := hold(cluster.Name, "cluster "+cluster.Name)
		if err != nil {
			return nil, err
		}
	}
	for _, cluster := range clusters {
		for i, name := range cluster.Servers {
			err := hold(name, fmt.Sprintf("server %d of cluster %s", i+1, cluster.Name))
			if err != nil {
				return nil, err
			}
		}
	}

	return clusters, nil
}
