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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/synadia-io/orbit.go/ntf/backend"
)

const (
	// inProcessBackendName is the name the in-process backend is registered
	// under, and the backend a Service uses when Options.Backend is empty.
	inProcessBackendName = "inprocess"

	// inProcessInternalHost is the host a proxy beside an in-process node dials
	// it on.
	inProcessInternalHost = "127.0.0.1"
)

// inProcessError is a failure of the in-process backend. It carries the
// response code and message the handler answers it with.
type inProcessError struct {
	// Code is the response code.
	Code string

	// Message is the response message.
	Message string

	// Err is the cause, nil for none.
	Err error
}

// Error returns the response message.
func (e *inProcessError) Error() string {
	return e.Message
}

// Unwrap returns the cause.
func (e *inProcessError) Unwrap() error {
	return e.Err
}

// errInstanceNotFound is the failure of a placement or provision of an
// instance a destroy or reset has removed.
var errInstanceNotFound = &inProcessError{Code: "404", Message: "Instance not found"}

// errNodeTornDown is the failure of a call on a node after Teardown of its
// instance.
var errNodeTornDown = errors.New("node torn down")

// errNoPersistedConfig is the cause of a Start of a node with no config to
// start from.
var errNoPersistedConfig = errors.New("no persisted config")

// errIdentityChange is the cause of an Apply refused because the revision
// changes the node's identity.
var errIdentityChange = errors.New("revision changes the node's identity")

// errPathOutsideNode is the cause of an Apply refused because a file of the
// revision does not lie in the node's directory.
var errPathOutsideNode = errors.New("file path outside the node's directory")

// inProcessBackend runs managed servers in the service's own process. It
// closes over the Service: ports come from its port range through reservePort
// and are recorded on the service's instance, and directories live under its
// directory.
type inProcessBackend struct {
	svc *Service

	// mu guards instances and every field of each record in it.
	mu        sync.Mutex
	instances map[string]*inProcessInstance
}

// inProcessInstance is what the in-process backend holds for one instance,
// from Placement until Teardown.
type inProcessInstance struct {
	// inst is the service's instance, which records the ports reserved for it.
	inst *instance

	// held are the listeners holding the ports placement reserved, keyed by
	// node and listener name, until the node starts.
	held map[[2]string]*net.TCPListener

	// nodes are the nodes Provision started, in plan order.
	nodes []*inProcessNode

	// torn is set by Teardown. Placement then closes each listener it reserves
	// at once and Provision starts no further node.
	torn bool

	// provisioning is closed when the instance's Provision returns, nil until
	// Provision is called. Teardown waits on it before it stops anything.
	provisioning chan struct{}

	// done is closed once Teardown has stopped and removed everything. A
	// Teardown that finds another in progress waits on it.
	done chan struct{}
}

// newInProcessBackend returns the in-process backend of s.
func newInProcessBackend(s *Service) *inProcessBackend {
	return &inProcessBackend{svc: s, instances: map[string]*inProcessInstance{}}
}

// Capabilities returns none: an in-process server can only be shut down
// gracefully, and runs the one nats-server version linked into the service.
func (b *inProcessBackend) Capabilities() []backend.Capability {
	return nil
}

// DefaultVersion returns the version of the linked nats-server.
func (b *inProcessBackend) DefaultVersion() string {
	return server.VERSION
}

// Placement returns the in-process placement of the registered instance
// instanceID. It fails with errInstanceNotFound when the instance is no longer
// registered.
func (b *inProcessBackend) Placement(_ context.Context, instanceID string) (backend.Placement, error) {
	b.svc.mu.Lock()
	inst, ok := b.svc.instances[instanceID]
	b.svc.mu.Unlock()

	if !ok {
		return nil, errInstanceNotFound
	}

	rec := &inProcessInstance{inst: inst, held: map[[2]string]*net.TCPListener{}, done: make(chan struct{})}

	b.mu.Lock()
	b.instances[instanceID] = rec
	b.mu.Unlock()

	return &inProcessPlacement{backend: b, rec: rec}, nil
}

// Provision writes the files of plan and starts its nodes in plan order, each
// on the listeners holding its ports, handed over just before it starts. On a
// failure it stops the nodes it started and returns an *inProcessError: 002 or
// 008 for a write, as writePlan answers it, 003 for a start, and
// errInstanceNotFound when Teardown of the instance ran first. A start that
// could not bind a port wraps ErrServerListen.
func (b *inProcessBackend) Provision(_ context.Context, plan *backend.InstancePlan) ([]backend.Node, error) {
	b.mu.Lock()
	rec, ok := b.instances[plan.ID]
	torn := ok && rec.torn
	if ok && !torn {
		rec.provisioning = make(chan struct{})
	}
	b.mu.Unlock()

	if !ok || torn {
		return nil, errInstanceNotFound
	}

	// A Teardown that marked the instance torn meanwhile waits for this.
	defer close(rec.provisioning)

	fail := func(err error) ([]backend.Node, error) {
		b.mu.Lock()
		started := rec.nodes
		rec.nodes = nil
		b.mu.Unlock()

		b.stopNodes(started)

		return nil, err
	}

	instDir := rec.inst.RootDir

	err := writePlan(plan, instDir)
	if err != nil {
		return nil, err
	}

	nodes := make([]backend.Node, 0, len(plan.Nodes))

	for _, n := range plan.Nodes {
		var held []*net.TCPListener

		// The node's listeners stay in rec.held for Teardown to close when the
		// instance is already torn, so they are taken only when it is not.
		b.mu.Lock()
		torn = rec.torn
		if !torn {
			for _, l := range n.Listeners {
				key := [2]string{n.Name, l.Name}
				ln, ok := rec.held[key]
				if ok {
					held = append(held, ln)
					delete(rec.held, key)
				}
			}
		}
		b.mu.Unlock()

		if torn {
			return fail(errInstanceNotFound)
		}

		dir := filepath.Join(instDir, n.Dir)

		srv, cfgPath, err := runServerWithConfig(b.svc.log, n.Config, dir, held)
		if err != nil {
			b.mu.Lock()
			torn = rec.torn
			b.mu.Unlock()

			if torn {
				return fail(errInstanceNotFound)
			}
			return fail(&inProcessError{Code: "003", Message: fmt.Sprintf("Server creation failed: %v", err), Err: err})
		}

		node := &inProcessNode{name: n.Name, addrs: b.addresses(n, srv), identity: nodeIdentity(n), instDir: instDir, cfgPath: cfgPath, log: b.svc.log, srv: srv}

		b.mu.Lock()
		torn = rec.torn
		if !torn {
			rec.nodes = append(rec.nodes, node)
		}
		b.mu.Unlock()

		// Teardown marked the instance torn while the node was starting. It
		// waits for this Provision to return, so stop the node here.
		if torn {
			b.stopNodes([]*inProcessNode{node})
			return fail(errInstanceNotFound)
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

// addresses returns the addresses of node n, which runs as srv. Every listener
// is reached on its planned port, apart from the client listener, whose port is
// read from srv, since a custom template can set its own.
func (b *inProcessBackend) addresses(n *backend.NodePlan, srv *server.Server) []backend.Address {
	addrs := make([]backend.Address, 0, len(n.Listeners))

	for _, l := range n.Listeners {
		port := l.Endpoint.Port
		if l.Name == "client" {
			var err error
			port, err = clientPortOf(srv)
			if err != nil {
				b.svc.log.Warn("Could not parse client port", "server", n.Name, "err", err)
			}
		}

		addrs = append(addrs, backend.Address{
			Name:     l.Name,
			Internal: backend.Endpoint{Host: inProcessInternalHost, Port: port},
			External: backend.Endpoint{Host: l.Endpoint.Host, Port: port},
		})
	}

	return addrs
}

// Teardown stops the instance's started nodes, closes the listeners still
// holding its ports, releases its ports and removes its directory unless the
// service preserves it. A Provision still starting the instance starts no
// further node, and Teardown waits for it to return before it stops anything.
// It also waits for a Start of one of the instance's nodes in flight to return.
// A Teardown that finds another in progress waits for that one to finish.
// Calling it once one has finished does nothing.
func (b *inProcessBackend) Teardown(_ context.Context, instanceID string) error {
	b.mu.Lock()
	rec, ok := b.instances[instanceID]
	inProgress := ok && rec.torn
	var provisioning chan struct{}
	if ok && !inProgress {
		rec.torn = true
		provisioning = rec.provisioning
	}
	b.mu.Unlock()

	if !ok {
		return nil
	}

	if inProgress {
		<-rec.done
		return nil
	}

	defer func() {
		b.mu.Lock()
		delete(b.instances, instanceID)
		b.mu.Unlock()

		close(rec.done)
	}()

	// Provision sees torn, stops the node it is starting and returns. Only
	// then are the nodes it started and the directory it writes in final.
	if provisioning != nil {
		<-provisioning
	}

	b.mu.Lock()
	nodes := rec.nodes
	rec.nodes = nil
	held := rec.held
	rec.held = nil
	b.mu.Unlock()

	for _, ln := range held {
		ln.Close()
	}

	b.stopNodes(nodes)

	inst := rec.inst

	b.svc.mu.Lock()
	ports := inst.ports
	inst.ports = nil
	b.svc.mu.Unlock()
	b.svc.ports.release(ports...)

	if b.svc.preserve {
		b.svc.log.Info("Preserving instance", "instance", inst.ID, "dir", inst.RootDir)
		return nil
	}

	b.svc.log.Info("Removing instance dir", "instance", inst.ID, "dir", inst.RootDir)
	err := os.RemoveAll(inst.RootDir)
	if err != nil {
		return fmt.Errorf("could not remove instance dir: %w", err)
	}

	return nil
}

// stopNodes marks each of nodes torn and shuts its server down. A Start of a
// node that swaps in its server before the node is marked torn has it shut down
// here, and one that would swap it in afterwards shuts it down itself. A Start
// in flight is waited for, with no lock held, so no server of the node runs
// once stopNodes returns.
func (b *inProcessBackend) stopNodes(nodes []*inProcessNode) {
	for _, n := range nodes {
		n.mu.Lock()
		n.torn = true
		starting := n.starting
		n.mu.Unlock()

		if starting != nil {
			<-starting
		}

		n.mu.Lock()
		srv := n.srv
		n.mu.Unlock()

		srv.Shutdown()
		srv.WaitForShutdown()
		b.svc.log.Info("Stopped server", "server", srv.Name())
	}
}

// Close does nothing: the in-process backend holds nothing for the whole
// service.
func (b *inProcessBackend) Close() error {
	return nil
}

// tlsFileLabels names each of an instance's TLS files, by its path in the plan,
// in the message of a failed write.
var tlsFileLabels = map[string]string{
	filepath.Join("tls", "ca.pem"):     "ca",
	filepath.Join("tls", "server.crt"): "server cert",
	filepath.Join("tls", "server.key"): "server key",
}

// writePlan creates instDir, the directory of each of the plan's instance
// files, and each node's directory and snippets directory, all with mode 0700,
// then writes every instance and node file with its mode.
//
// A failure is an *inProcessError: 008 for an instance file, which is TLS
// material, or the managed TLS snippet, 002 for anything else.
func writePlan(plan *backend.InstancePlan, instDir string) error {
	err := os.MkdirAll(instDir, 0700)
	if err != nil {
		return &inProcessError{Code: "002", Message: fmt.Sprintf("Failed to create instance dir: %v", err), Err: err}
	}

	for _, f := range plan.Files {
		err := os.MkdirAll(filepath.Join(instDir, filepath.Dir(f.Path)), 0700)
		if err != nil {
			return &inProcessError{Code: "008", Message: fmt.Sprintf("TLS setup failed: tls dir: %v", err), Err: err}
		}
	}

	for _, n := range plan.Nodes {
		err := os.MkdirAll(filepath.Join(instDir, n.Dir, "snippets"), 0700)
		if err != nil {
			return &inProcessError{Code: "002", Message: "Failed to create server directory", Err: err}
		}
	}

	for _, f := range plan.Files {
		err := os.WriteFile(filepath.Join(instDir, f.Path), f.Data, f.Mode)
		if err != nil {
			return &inProcessError{Code: "008", Message: fmt.Sprintf("TLS setup failed: write %s: %v", tlsFileLabels[f.Path], err), Err: err}
		}
	}

	for _, n := range plan.Nodes {
		for _, f := range n.Files {
			err := os.WriteFile(filepath.Join(instDir, f.Path), f.Data, f.Mode)
			if err != nil {
				name := filepath.Base(f.Path)
				if name == "_tls_managed.conf" {
					perr := &planError{Kind: planErrTLSSnippet, Err: err}
					return &inProcessError{Code: planErrorCode(perr), Message: perr.Error(), Err: err}
				}
				return &inProcessError{Code: "002", Message: fmt.Sprintf("Snippet failure: snippet %q write: %v", strings.TrimSuffix(name, ".conf"), err), Err: err}
			}
		}
	}

	return nil
}

// inProcessPlacement places an instance on this host, for nodes that run in the
// service's own process. Ports come from the service's port range and are
// recorded on the instance, so they return to the range when the instance is
// torn down. Directories live under the service's directory.
type inProcessPlacement struct {
	backend *inProcessBackend
	rec     *inProcessInstance
}

// ListenerAddress reserves a port from the service's port range and returns it
// on localhost. The listener holding the port is kept on the instance until
// the node starts. The node and listener names do not affect the port. An
// exhausted range returns an error wrapping ErrPortRangeExhausted.
func (p *inProcessPlacement) ListenerAddress(node, name string) (backend.Listener, error) {
	port, ln, err := p.backend.svc.reservePort(p.rec.inst)
	if err != nil {
		return backend.Listener{}, err
	}

	p.backend.mu.Lock()
	torn := p.rec.torn
	if !torn {
		p.rec.held[[2]string{node, name}] = ln
	}
	p.backend.mu.Unlock()

	// Teardown has already closed the instance's listeners and will not see
	// this one.
	if torn {
		ln.Close()
	}

	return backend.Listener{Name: name, Endpoint: backend.Endpoint{Host: inProcessHost, Port: port}}, nil
}

// InstanceDir returns <Dir>/<id>.
func (p *inProcessPlacement) InstanceDir() string {
	return p.rec.inst.RootDir
}

// StoreDir returns the node's directory under the instance directory: n<i> for
// a single server or a cluster node, c<c>_s<i> for a super-cluster node. It is
// the node's name without the "<short id>-" prefix the planner gives it.
func (p *inProcessPlacement) StoreDir(node string) string {
	return filepath.Join(p.InstanceDir(), strings.TrimPrefix(node, shortID(p.rec.inst.ID)+"-"))
}

// inProcessNode is one server the in-process backend started.
type inProcessNode struct {
	name  string
	addrs []backend.Address

	// identity is the node's plan with no files and no config, which Apply
	// compares a revision against.
	identity backend.NodePlan

	// instDir is the instance directory, which the paths of a revision's files
	// are relative to.
	instDir string

	cfgPath string
	log     *slog.Logger

	// mu guards srv, torn and starting. It is held only to read or swap them,
	// so Status never waits behind a start or stop, apart from Apply, which
	// holds it across its writes so Teardown cannot remove the directory
	// underneath them.
	mu  sync.Mutex
	srv *server.Server

	// torn is set by stopNodes, from Teardown of the node's instance or a
	// failed Provision. Every method that returns an error then fails with
	// errNodeTornDown.
	torn bool

	// starting is closed when a Start in flight returns, nil when none is.
	// stopNodes waits on it.
	starting chan struct{}
}

// nodeIdentity returns n with no files and no config, and its own copies of
// its listeners and persistent paths.
func nodeIdentity(n *backend.NodePlan) backend.NodePlan {
	id := *n
	id.Files = nil
	id.Config = nil
	id.Listeners = slices.Clone(n.Listeners)
	id.Persistent = slices.Clone(n.Persistent)

	return id
}

// identityChange names the first part of the node's identity, as Apply
// compares it, in which next differs from cur, or returns empty when none does.
func identityChange(cur, next *backend.NodePlan) string {
	switch {
	case next.Name != cur.Name:
		return "name"
	case next.Dir != cur.Dir:
		return "dir"
	case next.Cluster != cur.Cluster:
		return "cluster"
	case !slices.Equal(next.Listeners, cur.Listeners):
		return "listeners"
	case !slices.Equal(next.Persistent, cur.Persistent):
		return "persistent"
	default:
		return ""
	}
}

// pathInDir reports whether path, relative to the instance directory, is local
// and, once cleaned, is dir itself or lies under it. A sibling sharing dir's
// prefix, such as n1x for n1, is not under it.
func pathInDir(dir, path string) bool {
	if !filepath.IsLocal(path) {
		return false
	}

	rel, err := filepath.Rel(filepath.Clean(dir), filepath.Clean(path))
	if err != nil {
		return false
	}

	return filepath.IsLocal(rel)
}

// Name returns the node's plan name.
func (n *inProcessNode) Name() string {
	return n.name
}

// Addresses returns the node's addresses, fixed when it was provisioned.
func (n *inProcessNode) Addresses() []backend.Address {
	return n.addrs
}

// Status reads the node's state from its live server. A stopped server still
// answers its name and cluster, and has no cluster listener.
func (n *inProcessNode) Status(context.Context) (backend.NodeStatus, error) {
	n.mu.Lock()
	srv, torn := n.srv, n.torn
	n.mu.Unlock()

	if torn {
		return backend.NodeStatus{}, errNodeTornDown
	}

	st := backend.NodeStatus{
		ServerName: srv.Name(),
		Cluster:    srv.ClusterName(),
		Running:    srv.Running(),
		Version:    server.VERSION,
	}

	addr := srv.ClusterAddr()
	if addr != nil {
		st.ClusterPort = addr.Port
	}

	return st, nil
}

// Start starts a new server from the node's config and swaps it in for the
// stopped one, since a server cannot be restarted in place after a shutdown.
// When Teardown marked the node torn while the server was starting, it shuts
// that server down and fails; Teardown waits for it to return. A node with no
// config fails with an *inProcessError carrying 005.
func (n *inProcessNode) Start(context.Context) error {
	if n.cfgPath == "" {
		return &inProcessError{Code: "005", Message: "Server cannot be restarted: no persisted config", Err: errNoPersistedConfig}
	}

	n.mu.Lock()
	torn := n.torn
	starting := make(chan struct{})
	if !torn {
		n.starting = starting
	}
	n.mu.Unlock()

	if torn {
		return errNodeTornDown
	}

	defer func() {
		n.mu.Lock()
		n.starting = nil
		n.mu.Unlock()

		close(starting)
	}()

	srv, err := startFromConfig(n.log, n.cfgPath)
	if err != nil {
		return err
	}

	n.mu.Lock()
	torn = n.torn
	if !torn {
		n.srv = srv
	}
	n.mu.Unlock()

	// Teardown has stopped the server the node held and will not see this one.
	if torn {
		srv.Shutdown()
		srv.WaitForShutdown()
		return errNodeTornDown
	}

	return nil
}

// Stop shuts the node's server down and waits for it to finish.
func (n *inProcessNode) Stop(context.Context) error {
	n.mu.Lock()
	srv, torn := n.srv, n.torn
	n.mu.Unlock()

	if torn {
		return errNodeTornDown
	}

	srv.Shutdown()
	srv.WaitForShutdown()

	return nil
}

// Reload makes the node's server read its config again.
func (n *inProcessNode) Reload(context.Context) error {
	n.mu.Lock()
	srv, torn := n.srv, n.torn
	n.mu.Unlock()

	if torn {
		return errNodeTornDown
	}

	return srv.Reload()
}

// Apply writes the revision plan's files, each with its mode, then overwrites
// the node's config with its Config, under the node's mutex. Files the revision
// does not carry are kept. It never reloads and works on a stopped node.
//
// It refuses a revision that changes the node's name, directory, cluster,
// listeners or persistent paths with an error wrapping errIdentityChange that
// names the field, one with a file outside the node's Dir with an error
// wrapping errPathOutsideNode that names the path, and one that changes the
// version with backend.ErrUnsupported, since the backend does not declare
// CapabilityVersionChange. A refused revision writes nothing. A failed write is
// an *inProcessError carrying 009 and the message an update answers it with.
func (n *inProcessNode) Apply(_ context.Context, plan *backend.NodePlan) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.torn {
		return errNodeTornDown
	}

	field := identityChange(&n.identity, plan)
	if field != "" {
		return fmt.Errorf("%w: %s differs", errIdentityChange, field)
	}

	for _, f := range plan.Files {
		if !pathInDir(plan.Dir, f.Path) {
			return fmt.Errorf("%w: %s", errPathOutsideNode, f.Path)
		}
	}

	if plan.Version != n.identity.Version {
		return backend.ErrUnsupported
	}

	for _, f := range plan.Files {
		err := os.WriteFile(filepath.Join(n.instDir, f.Path), f.Data, f.Mode)
		if err != nil {
			name := filepath.Base(f.Path)
			if name == "_tls_managed.conf" {
				return &inProcessError{Code: "009", Message: fmt.Sprintf("TLS snippet rewrite failed: %v", err), Err: err}
			}
			return &inProcessError{Code: "009", Message: fmt.Sprintf("snippet render failed: snippet %q write: %v", strings.TrimSuffix(name, ".conf"), err), Err: err}
		}
	}

	err := os.WriteFile(n.cfgPath, plan.Config, 0600)
	if err != nil {
		return &inProcessError{Code: "009", Message: fmt.Sprintf("config write failed: %v", err), Err: err}
	}

	return nil
}

// Kill is not supported: an in-process server can only be shut down
// gracefully.
func (n *inProcessNode) Kill(context.Context) error {
	return backend.ErrUnsupported
}

// Pause is not supported.
func (n *inProcessNode) Pause(context.Context) error {
	return backend.ErrUnsupported
}

// Resume is not supported.
func (n *inProcessNode) Resume(context.Context) error {
	return backend.ErrUnsupported
}

var (
	_ backend.Backend   = (*inProcessBackend)(nil)
	_ backend.Placement = (*inProcessPlacement)(nil)
	_ backend.Node      = (*inProcessNode)(nil)
)
