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
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/nats-io/nuid"
)

const (
	// DefaultName is the micro service name the service registers under.
	DefaultName = "test-management"
	// DefaultGroup is the subject group every endpoint hangs off, so the create
	// server endpoint is "tester.create.server".
	DefaultGroup = "tester"
	// DefaultServerName names the embedded management server.
	DefaultServerName = "ntf-management"

	// serviceVersion is reported by micro discovery. It describes the endpoint
	// contract, not the release, so callers do not set it.
	serviceVersion = "0.0.1"

	// readyTimeout is how long New waits for the embedded server to accept
	// connections when the context carries no earlier deadline.
	readyTimeout = 10 * time.Second
)

// Options configures a Service. The zero value is usable: it starts an embedded
// nats-server on an OS-chosen port, keeps instance state in a temporary directory
// it removes on Close, logs nothing, and refuses trace capture requests.
//
// Embedding a Service grants every client that can reach its subjects the ability
// to run nats-server configurations of their choosing on this host. See the
// package documentation before exposing one on a shared connection.
type Options struct {
	// Conn hosts the service on the caller's connection. Nil starts an embedded
	// server per Embedded. Close never closes a connection supplied here.
	Conn *nats.Conn

	// Embedded configures the server started when Conn is nil.
	Embedded EmbeddedOptions

	// Dir holds per-instance state. Empty creates a temporary directory the
	// service owns and removes on Close unless Preserve is set. Read the
	// resolved value back with Dir.
	Dir string

	// Preserve keeps instance directories on teardown, for debugging.
	Preserve bool

	// AdvertiseHost is the host managed servers advertise to clients as
	// client_advertise. Empty advertises nothing.
	AdvertiseHost string

	// Logger receives a line per request and per managed server transition.
	// Nil discards.
	Logger *slog.Logger

	// Name is the micro service name. Empty uses DefaultName.
	Name string

	// Group is the subject group every endpoint hangs off. Empty uses
	// DefaultGroup. Changing it moves every subject, and clients that expect
	// the default will not find the service.
	Group string

	// NewCapturer supplies trace capture. Nil refuses requests that ask for it.
	NewCapturer NewCapturerFunc
}

// EmbeddedOptions configures the management server started when Options.Conn is nil.
type EmbeddedOptions struct {
	// Port to listen on. Zero and -1 both bind an OS-chosen port, which is what
	// a test suite wants; read the result back with ClientURL or Port.
	Port int

	// ServerName names the embedded server. Empty uses DefaultServerName.
	ServerName string

	// Log writes the embedded server's own log to stdout. Managed servers are
	// unaffected: they always log to a file under their instance directory.
	Log bool
}

// Service hosts the management API on a NATS connection: it creates, inspects and
// tears down managed nats-server instances on request.
type Service struct {
	// srv is the embedded management server, nil when hosting on a caller's conn.
	srv *server.Server
	nc  *nats.Conn
	// ownConn reports whether Close may close nc.
	ownConn bool

	micro    micro.Service
	log      *slog.Logger
	capturer Capturer

	dir string
	// ownDir reports whether Close may remove dir.
	ownDir        bool
	preserve      bool
	advertiseHost string

	mu        sync.Mutex
	instances map[string]*instance
}

type instance struct {
	ID          string
	Kind        string
	Description string
	Cluster     string
	RootDir     string
	Servers     []*managedServer
	Created     time.Time
}

type managedServer struct {
	srv        *server.Server
	rootDir    string
	configPath string
	ports      map[string]int

	// instanceID names the owning instance, so a handler holding only this
	// server can ask whether that instance is still registered. A start that
	// finished after a concurrent destroy would otherwise hand a running server
	// to an instance nobody will ever tear down.
	instanceID string

	// clientPort is the server's reserved client listen port, captured at create
	// time. Unlike the live server's address it survives a stop, so status can
	// report the client port even for a stopped node.
	clientPort int

	// advertiseHost is the resolved client_advertise host for this server,
	// captured at create time. Immutable for the life of the server (an update
	// never changes it), so status can read it lock-free rather than racing the
	// td pointer swap in updateServer. Empty when the node advertises nothing.
	advertiseHost string

	// td is the render environment captured at create time. Used by
	// updateServer to re-render the snippets and main template when staging
	// a new config.
	td *templateData

	// tlsFiles holds the generated-TLS material paths and rendered tls{} knobs
	// (verify, handshake_first, timeout) for this server. nil when the server
	// was not created with generated TLS. The pointer is shared across an
	// instance's nodes at create time; updateServer copy-on-writes a fresh copy
	// for the single targeted server when changing its timeout.
	tlsFiles *tlsInstanceFiles

	// traceProxy is the capture proxy fronting this server's client port, set when
	// the server was created with trace capture enabled. nil otherwise. Stopped
	// (flushing in-flight captures) before the server is shut down on teardown.
	traceProxy CaptureProxy

	// cfgMu guards the server's config (configPath and its snippet
	// files). Held by update, reload and start operations.
	cfgMu sync.Mutex
}

// New starts the management service and, unless Options.Conn is set, the embedded
// server that hosts it. It unwinds everything it started if any step fails, so a
// non-nil error leaves nothing running.
//
// ctx limits startup only; the service runs until Close.
func New(ctx context.Context, opts Options) (*Service, error) {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	s := &Service{
		log:           log,
		preserve:      opts.Preserve,
		advertiseHost: opts.AdvertiseHost,
		instances:     map[string]*instance{},
	}

	// Anything started below is registered here so a later failure can undo it
	// in reverse order.
	var unwind []func()
	fail := func(err error) (*Service, error) {
		for i := len(unwind) - 1; i >= 0; i-- {
			unwind[i]()
		}
		return nil, err
	}

	s.dir = opts.Dir
	if s.dir == "" {
		dir, err := os.MkdirTemp("", "ntf")
		if err != nil {
			return nil, fmt.Errorf("could not create instance dir: %w", err)
		}
		s.dir = dir
		s.ownDir = true
		unwind = append(unwind, func() { os.RemoveAll(dir) })
	}

	switch {
	case opts.Conn != nil:
		s.nc = opts.Conn

	default:
		srv, err := s.startEmbedded(ctx, opts.Embedded)
		if err != nil {
			return fail(err)
		}
		s.srv = srv
		unwind = append(unwind, func() {
			srv.Shutdown()
			srv.WaitForShutdown()
		})

		nc, err := nats.Connect("", nats.InProcessServer(srv))
		if err != nil {
			return fail(fmt.Errorf("could not connect to the management server: %w", err))
		}
		s.nc = nc
		s.ownConn = true
		unwind = append(unwind, nc.Close)
	}

	group := opts.Group
	if group == "" {
		group = DefaultGroup
	}
	name := opts.Name
	if name == "" {
		name = DefaultName
	}

	if err := s.startServices(name, group); err != nil {
		return fail(err)
	}
	unwind = append(unwind, func() { s.micro.Stop() })

	if opts.NewCapturer != nil {
		capturer, err := opts.NewCapturer(ctx, s)
		if err != nil {
			return fail(fmt.Errorf("could not create the trace capturer: %w", err))
		}
		s.capturer = capturer
	}

	return s, nil
}

// startEmbedded starts the management server and waits for it to accept connections.
func (s *Service) startEmbedded(ctx context.Context, opts EmbeddedOptions) (*server.Server, error) {
	name := opts.ServerName
	if name == "" {
		name = DefaultServerName
	}

	// nats-server spells an OS-chosen port -1 and reads 0 as "use 4222". A test
	// suite wants a free port far more often than it wants 4222, so both mean
	// OS-chosen here and a fixed port has to be asked for.
	port := opts.Port
	if port == 0 {
		port = server.RANDOM_PORT
	}

	srv, err := server.NewServer(&server.Options{
		ServerName: name,
		Port:       port,
		StoreDir:   filepath.Join(s.dir, "management"),
		NoSigs:     true,
		NoLog:      !opts.Log,
	})
	if err != nil {
		return nil, err
	}

	if opts.Log {
		srv.ConfigureLogger()
	}
	srv.Start()

	wait := readyTimeout
	deadline, ok := ctx.Deadline()
	if ok {
		wait = time.Until(deadline)
	}
	if !srv.ReadyForConnections(wait) {
		srv.Shutdown()
		srv.WaitForShutdown()
		return nil, fmt.Errorf("management server not ready for connections after %v", wait)
	}

	return srv, nil
}

func (s *Service) startServices(name, group string) error {
	var err error

	s.micro, err = micro.AddService(s.nc, micro.Config{
		Name:    name,
		Version: serviceVersion,
	})
	if err != nil {
		return fmt.Errorf("failed to add micro service: %w", err)
	}

	srv := s.micro.AddGroup(group)

	create := srv.AddGroup("create")
	err = create.AddEndpoint("server", micro.HandlerFunc(s.createServer))
	if err != nil {
		return err
	}
	err = create.AddEndpoint("cluster", micro.HandlerFunc(s.createCluster))
	if err != nil {
		return err
	}
	err = create.AddEndpoint("super-cluster", micro.HandlerFunc(s.createSuperCluster))
	if err != nil {
		return err
	}

	stop := srv.AddGroup("stop")
	err = stop.AddEndpoint("server", micro.HandlerFunc(s.stopServer))
	if err != nil {
		return err
	}
	err = stop.AddEndpoint("instance", micro.HandlerFunc(s.stopInstance))
	if err != nil {
		return err
	}

	start := srv.AddGroup("start")
	err = start.AddEndpoint("server", micro.HandlerFunc(s.startServer))
	if err != nil {
		return err
	}
	err = start.AddEndpoint("instance", micro.HandlerFunc(s.startInstance))
	if err != nil {
		return err
	}

	update := srv.AddGroup("update")
	err = update.AddEndpoint("server", micro.HandlerFunc(s.updateServer))
	if err != nil {
		return err
	}

	reload := srv.AddGroup("reload")
	err = reload.AddEndpoint("server", micro.HandlerFunc(s.reloadServer))
	if err != nil {
		return err
	}

	err = srv.AddEndpoint("status", micro.HandlerFunc(s.status))
	if err != nil {
		return err
	}

	err = srv.AddEndpoint("reset", micro.HandlerFunc(s.reset))
	if err != nil {
		return err
	}

	err = srv.AddEndpoint("destroy", micro.HandlerFunc(s.destroy))
	if err != nil {
		return err
	}

	err = srv.AddEndpoint("list", micro.HandlerFunc(s.list))
	if err != nil {
		return err
	}

	return nil
}

// ManagementConn is the connection the service answers requests on. It belongs to
// the caller when one was supplied in Options.
func (s *Service) ManagementConn() *nats.Conn { return s.nc }

// EmbeddedServer is the management server the service started, or nil when it runs
// on a caller-supplied connection.
func (s *Service) EmbeddedServer() *server.Server { return s.srv }

// ClientURL is the address clients connect to. It is the embedded server's URL, or
// the URL of the caller-supplied connection.
func (s *Service) ClientURL() string {
	if s.srv != nil {
		return s.srv.ClientURL()
	}
	return s.nc.ConnectedUrl()
}

// Port is the port the embedded server listens on, or 0 when the service runs on a
// caller-supplied connection.
func (s *Service) Port() int {
	if s.srv == nil {
		return 0
	}
	port, err := clientPortOf(s.srv)
	if err != nil {
		return 0
	}
	return port
}

// Dir is the resolved directory holding per-instance state, whether it came from
// Options.Dir or was created by the service.
func (s *Service) Dir() string { return s.dir }

// Reset tears down every instance and keeps serving.
func (s *Service) Reset() error {
	s.mu.Lock()
	all := slices.Collect(maps.Values(s.instances))
	s.instances = map[string]*instance{}
	s.mu.Unlock()

	for _, inst := range all {
		s.tearDownInstance(inst)
	}

	return nil
}

// Close stops serving and releases everything the service owns: it stops answering
// requests, tears down every instance (flushing captures while the connection is
// still up), closes the capturer, and shuts down the embedded server. A connection
// supplied through Options is left open, as is a directory supplied through
// Options.Dir.
func (s *Service) Close() error {
	var errs []error

	if s.micro != nil {
		if err := s.micro.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("could not stop the micro service: %w", err))
		}
	}

	if err := s.Reset(); err != nil {
		errs = append(errs, err)
	}

	if s.capturer != nil {
		if err := s.capturer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("could not close the trace capturer: %w", err))
		}
	}

	if s.ownConn {
		s.nc.Close()
	}

	if s.srv != nil {
		s.srv.Shutdown()
		s.srv.WaitForShutdown()
	}

	if s.ownDir && !s.preserve {
		if err := os.RemoveAll(s.dir); err != nil {
			errs = append(errs, fmt.Errorf("could not remove the instance dir: %w", err))
		}
	}

	return errors.Join(errs...)
}

// newInstance allocates a fresh instance, registers it under the lock, and returns it.
// The caller is responsible for filling Servers and removing the instance on failure
// (via dropInstance).
func (s *Service) newInstance(kind, description string) *instance {
	id := nuid.Next()
	inst := &instance{
		ID:          id,
		Kind:        kind,
		Description: description,
		RootDir:     filepath.Join(s.dir, id),
		Created:     time.Now(),
	}

	s.mu.Lock()
	s.instances[id] = inst
	s.mu.Unlock()

	return inst
}

// dropInstance removes a partially-built instance after a create failure.
// Shuts down whatever servers managed to start, removes the rootDir if not preserving.
func (s *Service) dropInstance(id string) {
	s.mu.Lock()
	inst, ok := s.instances[id]
	if ok {
		delete(s.instances, id)
	}
	s.mu.Unlock()

	if !ok {
		return
	}

	s.tearDownInstance(inst)
}

func (s *Service) tearDownInstance(inst *instance) {
	for _, ms := range inst.Servers {
		// Stop the capture proxy first, while the connection the capturer stores
		// through is still up, so in-flight captures flush before the backend
		// server goes away.
		if ms.traceProxy != nil {
			ms.traceProxy.Stop()
		}
		if ms.srv != nil {
			ms.srv.Shutdown()
			ms.srv.WaitForShutdown()
			s.log.Info("Stopped server", "server", ms.srv.Name())
		}
	}

	if inst.RootDir == "" {
		return
	}

	if s.preserve {
		s.log.Info("Preserving instance", "instance", inst.ID, "dir", inst.RootDir)
		return
	}

	s.log.Info("Removing instance dir", "instance", inst.ID, "dir", inst.RootDir)
	if err := os.RemoveAll(inst.RootDir); err != nil {
		s.log.Error("Failed to remove instance dir", "instance", inst.ID, "err", err)
	}
}

// shortID returns a short, name-safe identifier derived from a nuid. A nuid is
// 22 chars of [A-Za-z0-9]: a 12-char per-process prefix followed by a 10-char
// monotonic sequence. We slice from the tail so consecutive calls in the same
// process produce distinct shortIDs.
func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[len(id)-8:]
}

var _ io.Closer = (*Service)(nil)
