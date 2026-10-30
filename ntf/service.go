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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
	"github.com/nats-io/nuid"

	"github.com/synadia-io/orbit.go/ntf/agent"
	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/backend"
	"github.com/synadia-io/orbit.go/ntf/events"
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

	// defaultPortLow and defaultPortHigh bound the managed server port range when
	// Options.PortRange is not set.
	defaultPortLow  = 20000
	defaultPortHigh = 40000

	// defaultEventsMaxAge and defaultEventsMaxBytes bound the events stream
	// when Options.EventsMaxAge and Options.EventsMaxBytes are not set.
	defaultEventsMaxAge   = 24 * time.Hour
	defaultEventsMaxBytes = 1 << 30
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

	// PortRange bounds the ports the service hands to managed servers: client,
	// route, gateway and snippet listener ports. The zero value uses 20000 to
	// 40000, trimmed to end below the OS ephemeral port range when that range
	// starts inside it; New fails on any other overlap with the default. A range
	// set here must not overlap the ephemeral range or New fails:
	// outgoing connections of any process on the host draw ports from it, and one
	// can take a port between the service reserving it and a server binding it.
	// The overlap is checked on macOS and Linux only.
	PortRange PortRange

	// Logger receives a line per request and per managed server transition.
	// Nil discards.
	Logger *slog.Logger

	// Name is the micro service name. Empty uses DefaultName.
	Name string

	// Group is the subject group every endpoint hangs off. Empty uses
	// DefaultGroup. Changing it moves every subject, and clients that expect
	// the default will not find the service. It also names the events stream,
	// so New fails on a group that is not a single lowercase subject token
	// valid in a stream name, as events.ValidateGroup checks.
	Group string

	// EventsMaxAge is how long the events stream keeps an event before
	// discarding it. Zero keeps events for 24 hours.
	EventsMaxAge time.Duration

	// EventsMaxBytes is the size past which the events stream discards its
	// oldest events. Zero keeps up to 1 GiB.
	EventsMaxBytes int64

	// Backends are backend factories to register, keyed by name, beside the
	// built-in "inprocess" backend. New fails on an empty name or on
	// "inprocess".
	Backends map[string]backend.Factory

	// Backend names the registered backend that runs managed servers. Empty
	// uses "inprocess". New fails on a name that is not registered.
	Backend string

	// Agents are network agent factories to register, keyed by name. New
	// fails on an empty name. There is no built-in agent.
	Agents map[string]agent.Factory

	// Agent names the registered agent that fronts managed servers for trace
	// capture and shaping. Empty selects none, and requests that ask for trace
	// capture are refused. New fails on a name that is not registered.
	Agent string
}

// EmbeddedOptions configures the management server started when Options.Conn is nil.
type EmbeddedOptions struct {
	// Port to listen on. Zero and -1 both bind an OS-chosen port, which is what
	// a test suite wants; read the result back with ClientURL or Port.
	Port int

	// ServerName names the embedded server. Empty uses DefaultServerName.
	ServerName string

	// StoreDir is the embedded server's JetStream store directory, which
	// holds the events stream. Empty uses the management directory under
	// Options.Dir.
	StoreDir string

	// Log writes the embedded server's own log to stdout. Managed servers are
	// unaffected: they always log to a file under their instance directory.
	Log bool
}

// PortRange is an inclusive range of TCP ports.
type PortRange struct {
	Low  int
	High int
}

// Service hosts the management API on a NATS connection: it creates, inspects and
// tears down managed nats-server instances on request.
type Service struct {
	// srv is the embedded management server, nil when hosting on a caller's conn.
	srv *server.Server
	nc  *nats.Conn
	// ownConn reports whether Close may close nc.
	ownConn bool

	micro micro.Service
	log   *slog.Logger

	// agents starts the network agents of instances created with trace
	// capture, nil when no agent is selected.
	agents agent.Provider
	// agentName is the name agents was registered under. Status reports it.
	agentName string

	dir string
	// ownDir reports whether Close may remove dir.
	ownDir        bool
	preserve      bool
	advertiseHost string

	// ports hands out every port a managed server listens on.
	ports *portAllocator

	// backend runs the managed servers of every instance.
	backend backend.Backend
	// backendName is the name backend was registered under. Status reports it.
	backendName string

	// group is the subject group of the endpoints and the events.
	group string

	// js stores the service's events in the stream named stream, with an
	// ack. Nil when the service publishes its events live only.
	js     jetstream.JetStream
	stream string

	// agentEvents is the publisher handed to the agent.
	agentEvents *agentPublisher

	mu        sync.Mutex
	instances map[string]*instance

	// detachedRequests counts the requests perRequest runs in goroutines of
	// their own, for the stop, start and reload of a server and for the toxic
	// endpoints, so Close waits for them before it tears the instances down.
	// Add is called only under detachedRequestsMu while
	// detachedRequestsClosed is false, so no Add races the Wait in Close.
	detachedRequests       sync.WaitGroup
	detachedRequestsMu     sync.Mutex
	detachedRequestsClosed bool
}

type instance struct {
	ID          string
	Kind        string
	Description string
	Cluster     string
	RootDir     string
	Servers     []*managedServer
	Created     time.Time

	// ports lists every port reserved for the instance's servers. They stay
	// reserved while a server is stopped, so it restarts on the same ports, and
	// return to the range when the instance is torn down. Guarded by Service.mu.
	ports []int

	// plan is the plan the instance was created from, set once its nodes are
	// provisioned and before they are published. It holds no TLS material and
	// no contents of any file or config. Guarded by Service.mu.
	plan *instancePlan

	// links are the proxied links of an instance created with toxics, as its
	// create response lists them, set with Servers and never changed after.
	// Nil for any other instance. Guarded by Service.mu.
	links []api.Link

	// agent is the instance's network agent, set with Servers when its create
	// started one, for trace, proxy or toxics, and never changed after. A
	// teardown stops it once, before the servers go, so in-flight captures
	// flush. Nil for any other instance. Guarded by Service.mu.
	agent agent.Agent

	// captured reports whether agent fronts every server's client port with a
	// capture proxy, for trace or proxy, so the shaping calls act on it. Set
	// with agent and never changed after. Guarded by Service.mu.
	captured bool

	// events is the instance's event lock. A create holds it from making its
	// servers visible until created and ready are published, and a teardown
	// from stopping the agents until destroyed is published, so no node event
	// lands before created or after destroyed. A node handler holds it only
	// around its publish; its node's cfgMu orders that node's events. It is
	// taken before Service.mu, never while holding it.
	events sync.Mutex

	// announced reports whether the created event was published, so a
	// teardown records destroyed only for an instance it announced. Guarded
	// by events.
	announced bool

	// sourceSeq is the source_seq of the last event the service published for
	// the instance. Guarded by events.
	sourceSeq uint64

	// eventsSeq is the highest stream sequence acked for the service's events
	// of the instance, 0 for none. Guarded by Service.mu.
	eventsSeq uint64
}

type managedServer struct {
	// backendNode is the server as the backend runs it.
	backendNode backend.Node

	// rootDir is the server's directory under its placement. Only tests read
	// it.
	rootDir string
	ports   map[string]int

	// instanceID names the owning instance, so a handler holding only this
	// server can ask whether that instance is still registered. A start that
	// finished after a concurrent destroy would otherwise hand a running server
	// to an instance nobody will ever tear down.
	instanceID string

	// clientPort is the server's reserved client listen port, or its client
	// proxy's port when its instance was created with toxics, captured at
	// create time. Unlike the live server's address it survives a stop, so
	// status can report the client port even for a stopped node.
	clientPort int

	// advertiseHost is the resolved client_advertise host for this server,
	// captured at create time. Immutable for the life of the server (an update
	// never changes it), so status can read it lock-free rather than racing the
	// node plan swap in updateServer. Empty when the node advertises nothing.
	advertiseHost string

	// node is this server's node in its instance's plan. updateServer renders
	// the snippets and main template from its template data and its managed TLS
	// settings when staging a new config. The TLS settings pointer is shared by
	// every node of the instance, so a node plan is never mutated: updateServer
	// swaps in a changed copy on success. Guarded by cfgMu.
	node *nodePlan

	// toxics reports whether the server's instance was created with toxics,
	// so an update checks a custom template, and the config it renders, for
	// the proxies. Immutable for the life of the server.
	toxics bool

	// cfgMu guards the server's config (its config path and its snippet
	// files). Held by update, reload, start and stop operations. Requests for
	// different servers run at once, so cfgMu is what keeps the stop, start
	// and reload requests of one server apart.
	cfgMu sync.Mutex
}

// New starts the management service and, unless Options.Conn is set, the embedded
// server that hosts it. It unwinds everything it started if any step fails, so a
// non-nil error leaves nothing running. On a caller's connection the events
// stream it created or updated stays, since other services may share it, and a
// later New reuses it.
//
// ctx limits startup only; the service runs until Close.
func New(ctx context.Context, opts Options) (*Service, error) {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	group := opts.Group
	if group == "" {
		group = DefaultGroup
	}
	err := events.ValidateGroup(group)
	if err != nil {
		return nil, err
	}

	s := &Service{
		log:           log,
		preserve:      opts.Preserve,
		advertiseHost: opts.AdvertiseHost,
		group:         group,
		instances:     map[string]*instance{},
	}

	portRange, err := resolvePortRange(opts.PortRange, ephemeralPortRange, log)
	if err != nil {
		return nil, err
	}
	s.ports = newPortAllocator(portRange)

	factories := map[string]backend.Factory{
		inProcessBackendName: func(context.Context, backend.Options) (backend.Backend, error) {
			return newInProcessBackend(s), nil
		},
	}
	for name, factory := range opts.Backends {
		if name == "" {
			return nil, fmt.Errorf("%w: empty name", ErrBackendName)
		}
		_, exists := factories[name]
		if exists {
			return nil, fmt.Errorf("%w: %q is already registered", ErrBackendName, name)
		}
		factories[name] = factory
	}

	backendName := opts.Backend
	if backendName == "" {
		backendName = inProcessBackendName
	}
	factory, ok := factories[backendName]
	if !ok {
		return nil, fmt.Errorf("%w: %q, registered: %s", ErrUnknownBackend, backendName, strings.Join(slices.Sorted(maps.Keys(factories)), ", "))
	}

	for name, f := range opts.Agents {
		if name == "" {
			return nil, fmt.Errorf("%w: empty name", ErrAgentName)
		}
		if f == nil {
			return nil, fmt.Errorf("%w: %q has a nil factory", ErrAgentName, name)
		}
	}

	var agentFactory agent.Factory
	if opts.Agent != "" {
		agentFactory, ok = opts.Agents[opts.Agent]
		if !ok {
			return nil, fmt.Errorf("%w: %q, registered: %s", ErrUnknownAgent, opts.Agent, strings.Join(slices.Sorted(maps.Keys(opts.Agents)), ", "))
		}
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

	be, err := factory(ctx, backend.Options{Dir: s.dir, Preserve: s.preserve, Logger: log})
	if err != nil {
		return fail(fmt.Errorf("could not create the %q backend: %w", backendName, err))
	}
	s.backend = be
	s.backendName = backendName
	unwind = append(unwind, func() { be.Close() })

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

	// The stream exists before the endpoints go live, so no event is lost.
	err = s.startEvents(ctx, opts)
	if err != nil {
		return fail(err)
	}
	s.agentEvents = newAgentPublisher(s.nc, s.group)

	// The agent is built before the endpoints go live, so no request finds the
	// service without it.
	if agentFactory != nil {
		provider, err := agentFactory(ctx, agent.Options{Dir: s.dir, Preserve: s.preserve, Logger: log, Conn: s.nc, Server: s.srv, Events: s.agentEvents})
		if err != nil {
			return fail(fmt.Errorf("could not create the %q agent: %w", opts.Agent, err))
		}
		if provider == nil {
			return fail(fmt.Errorf("could not create the %q agent: the factory returned no provider", opts.Agent))
		}
		s.agents = provider
		s.agentName = opts.Agent
		unwind = append(unwind, func() { provider.Close() })
	}

	name := opts.Name
	if name == "" {
		name = DefaultName
	}

	if err := s.startServices(name, group); err != nil {
		return fail(err)
	}
	unwind = append(unwind, func() { s.micro.Stop() })

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

	storeDir := opts.StoreDir
	if storeDir == "" {
		storeDir = filepath.Join(s.dir, "management")
	}

	srv, err := server.NewServer(&server.Options{
		ServerName: name,
		Port:       port,
		StoreDir:   storeDir,
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

	// JetStream is enabled once the server is up rather than in Start, which on
	// failure stops the server, or exits the process when it logs. Its store
	// is the jetstream directory under storeDir.
	err = srv.EnableJetStream(&server.JetStreamConfig{StoreDir: storeDir})
	if err != nil {
		srv.Shutdown()
		srv.WaitForShutdown()
		return nil, fmt.Errorf("could not enable JetStream on the management server in %s: %w", storeDir, err)
	}

	return srv, nil
}

// startEvents creates or updates the events stream of the service's group. A
// failure fails New on the embedded server. On a caller's connection, where
// JetStream may be missing or the stream may not be allowed, it logs a warning
// and leaves the service publishing its events live only.
func (s *Service) startEvents(ctx context.Context, opts Options) error {
	maxAge := opts.EventsMaxAge
	if maxAge == 0 {
		maxAge = defaultEventsMaxAge
	}
	maxBytes := opts.EventsMaxBytes
	if maxBytes == 0 {
		maxBytes = defaultEventsMaxBytes
	}

	cfg := jetstream.StreamConfig{
		Name:     events.StreamName(s.group),
		Subjects: []string{events.StreamSubject(s.group)},
		Storage:  jetstream.FileStorage,
		Discard:  jetstream.DiscardOld,
		MaxAge:   maxAge,
		MaxBytes: maxBytes,
	}

	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()

	js, err := jetstream.New(s.nc)
	if err == nil {
		_, err = js.CreateOrUpdateStream(ctx, cfg)
	}
	if err != nil {
		if s.srv != nil {
			return fmt.Errorf("could not create the events stream %s: %w", cfg.Name, err)
		}
		s.log.Warn("Publishing events live only, could not create the events stream", "stream", cfg.Name, "err", err)
		return nil
	}

	s.js = js
	s.stream = cfg.Name

	return nil
}

func (s *Service) startServices(name, group string) error {
	var err error

	requestSchemas, err := compileRequestSchemas()
	if err != nil {
		return err
	}
	schemas, err := endpointSchemas()
	if err != nil {
		return err
	}

	// check wraps the handler of the endpoint name, as api.Endpoints names it,
	// so its request is validated before the handler runs, and notes the
	// endpoint so every endpoint is known to be validated once all are added.
	checked := map[string]bool{}
	check := func(name string, h micro.HandlerFunc) micro.HandlerFunc {
		checked[name] = true
		return validated(requestSchemas[name], emptyBodyEndpoints[name], h)
	}

	s.micro, err = micro.AddService(s.nc, micro.Config{
		Name:    name,
		Version: serviceVersion,
	})
	if err != nil {
		return fmt.Errorf("failed to add micro service: %w", err)
	}

	srv := s.micro.AddGroup(group)

	create := srv.AddGroup("create")
	err = create.AddEndpoint("server", check("create.server", s.createServer))
	if err != nil {
		return err
	}
	err = create.AddEndpoint("cluster", check("create.cluster", s.createCluster))
	if err != nil {
		return err
	}
	err = create.AddEndpoint("super-cluster", check("create.super-cluster", s.createSuperCluster))
	if err != nil {
		return err
	}

	stop := srv.AddGroup("stop")
	err = stop.AddEndpoint("server", check("stop.server", s.perRequest(s.stopServer)))
	if err != nil {
		return err
	}
	err = stop.AddEndpoint("instance", check("stop.instance", s.stopInstance))
	if err != nil {
		return err
	}

	start := srv.AddGroup("start")
	err = start.AddEndpoint("server", check("start.server", s.perRequest(s.startServer)))
	if err != nil {
		return err
	}
	err = start.AddEndpoint("instance", check("start.instance", s.startInstance))
	if err != nil {
		return err
	}

	update := srv.AddGroup("update")
	err = update.AddEndpoint("server", check("update.server", s.updateServer))
	if err != nil {
		return err
	}

	reload := srv.AddGroup("reload")
	err = reload.AddEndpoint("server", check("reload.server", s.perRequest(s.reloadServer)))
	if err != nil {
		return err
	}

	shape := srv.AddGroup("shape")
	err = shape.AddEndpoint("set", check("shape.set", s.shapeSet))
	if err != nil {
		return err
	}
	err = shape.AddEndpoint("clear", check("shape.clear", s.shapeClear))
	if err != nil {
		return err
	}
	err = shape.AddEndpoint("report", check("shape.report", s.shapeReport))
	if err != nil {
		return err
	}
	err = shape.AddEndpoint("list", check("shape.list", s.shapeList))
	if err != nil {
		return err
	}

	toxic := srv.AddGroup("toxic")
	err = toxic.AddEndpoint("add", check("toxic.add", s.perRequest(s.toxicAdd)))
	if err != nil {
		return err
	}
	err = toxic.AddEndpoint("remove", check("toxic.remove", s.perRequest(s.toxicRemove)))
	if err != nil {
		return err
	}
	err = toxic.AddEndpoint("clear", check("toxic.clear", s.perRequest(s.toxicClear)))
	if err != nil {
		return err
	}
	err = toxic.AddEndpoint("report", check("toxic.report", s.perRequest(s.toxicReport)))
	if err != nil {
		return err
	}

	err = srv.AddEndpoint("status", check("status", s.status))
	if err != nil {
		return err
	}

	err = srv.AddEndpoint("reset", check("reset", s.reset))
	if err != nil {
		return err
	}

	err = srv.AddEndpoint("destroy", check("destroy", s.destroy))
	if err != nil {
		return err
	}

	err = srv.AddEndpoint("list", check("list", s.list))
	if err != nil {
		return err
	}

	err = srv.AddEndpoint("clock", check("clock", s.clock))
	if err != nil {
		return err
	}

	err = srv.AddEndpoint("schema", check("schema", schemaHandler(schemas)))
	if err != nil {
		return err
	}

	for _, ep := range api.Endpoints {
		if !checked[ep.Name] {
			return fmt.Errorf("endpoint %s is not added with request validation", ep.Name)
		}
	}

	return nil
}

// perRequest returns the handler of an endpoint whose requests each act on one
// node or one instance: stop.server, start.server, reload.server and the
// toxic endpoints. A micro endpoint handles one request at a time, so a
// request for one node or instance would otherwise wait behind a slow request
// for another, and a stop or a toxic add queued that way could run after the
// request undoing it had already answered. perRequest runs h for each request
// in a goroutine of its own instead. The node's cfgMu keeps the requests for
// one node apart. The toxic requests for one instance may reach its agent at
// once and in any order, which agent.Toxics allows: its agents accept
// concurrent calls.
//
// The goroutine answers through a detachedRequest, since the micro service
// reads its own request once the handler returns. Once Close has begun, a
// request still drained from the endpoint runs in the handler, as every other
// endpoint's does.
func (s *Service) perRequest(h micro.HandlerFunc) micro.HandlerFunc {
	return func(req micro.Request) {
		s.detachedRequestsMu.Lock()
		closed := s.detachedRequestsClosed
		if !closed {
			s.detachedRequests.Add(1)
		}
		s.detachedRequestsMu.Unlock()

		if closed {
			h(req)
			return
		}

		r := &detachedRequest{
			nc:      s.nc,
			subject: req.Subject(),
			reply:   req.Reply(),
			data:    req.Data(),
			headers: req.Headers(),
		}

		go func() {
			defer s.detachedRequests.Done()
			h(r)
		}()
	}
}

// detachedRequest is a micro.Request a handler can answer after the micro
// handler returned. It publishes its answer on the management connection
// itself, with the headers micro sets, and returns the errors micro's own
// request returns. It leaves the micro service's request alone, so the
// service's stats do not count its errors, and report a processing time near
// zero, since micro times only the start of the goroutine.
type detachedRequest struct {
	nc      *nats.Conn
	subject string
	reply   string
	data    []byte
	headers micro.Headers
}

var _ micro.Request = (*detachedRequest)(nil)

func (r *detachedRequest) Respond(data []byte, opts ...micro.RespondOpt) error {
	msg := &nats.Msg{Data: data}
	for _, opt := range opts {
		opt(msg)
	}

	// micro formats the cause with %s, so only ErrRespond matches errors.Is.
	err := r.publish(msg)
	if err != nil {
		return fmt.Errorf("%w: %s", micro.ErrRespond, err)
	}

	return nil
}

func (r *detachedRequest) RespondJSON(v any, opts ...micro.RespondOpt) error {
	data, err := json.Marshal(v)
	if err != nil {
		return micro.ErrMarshalResponse
	}

	return r.Respond(data, opts...)
}

func (r *detachedRequest) Error(code, description string, data []byte, opts ...micro.RespondOpt) error {
	if code == "" {
		return fmt.Errorf("%w: error code", micro.ErrArgRequired)
	}
	if description == "" {
		return fmt.Errorf("%w: description", micro.ErrArgRequired)
	}

	msg := &nats.Msg{
		Header: nats.Header{
			micro.ErrorHeader:     []string{description},
			micro.ErrorCodeHeader: []string{code},
		},
	}
	for _, opt := range opts {
		opt(msg)
	}
	msg.Data = data

	return r.publish(msg)
}

func (r *detachedRequest) Data() []byte { return r.data }

func (r *detachedRequest) Headers() micro.Headers { return r.headers }

func (r *detachedRequest) Subject() string { return r.subject }

func (r *detachedRequest) Reply() string { return r.reply }

// publish sends msg to the request's reply subject, returning the publish
// error as it is: Respond wraps it and Error does not, as micro's request does.
func (r *detachedRequest) publish(msg *nats.Msg) error {
	if r.reply == "" {
		return nats.ErrMsgNoReply
	}

	msg.Subject = r.reply

	return r.nc.PublishMsg(msg)
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
	return s.resetWith(events.ReasonReset)
}

// resetWith tears down every instance, recording reason in the destroyed event
// of each instance that was announced.
func (s *Service) resetWith(reason events.DestroyReason) error {
	s.mu.Lock()
	all := slices.Collect(maps.Values(s.instances))
	s.instances = map[string]*instance{}
	s.mu.Unlock()

	for _, inst := range all {
		s.tearDownInstance(inst, reason)
	}

	return nil
}

// Close stops serving and releases everything the service owns: it stops answering
// requests, waits for the server stop, start and reload requests and the toxic
// requests still running, tears down every instance (flushing captures while
// the connection is still up), closes the agent provider, and shuts down the
// embedded server. A connection supplied through Options is left open, as is a
// directory supplied through Options.Dir.
func (s *Service) Close() error {
	var errs []error

	if s.micro != nil {
		if err := s.micro.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("could not stop the micro service: %w", err))
		}
	}

	// Wait for the requests still running in their own goroutines, so none
	// acts on an instance while it is torn down below.
	s.detachedRequestsMu.Lock()
	s.detachedRequestsClosed = true
	s.detachedRequestsMu.Unlock()
	s.detachedRequests.Wait()

	// The connection and JetStream are still up, so the destroyed events are
	// stored.
	if err := s.resetWith(events.ReasonClose); err != nil {
		errs = append(errs, err)
	}

	err := s.backend.Close()
	if err != nil {
		errs = append(errs, fmt.Errorf("could not close the backend: %w", err))
	}

	if s.agents != nil {
		err := s.agents.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("could not close the agent provider: %w", err))
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

// dropInstance removes a partially-built instance after a create failure and
// tears it down. It tears it down even when a concurrent destroy or reset has
// already removed it, since the create may have placed or started more since
// that teardown. The create never announced the instance, so it records no
// event.
func (s *Service) dropInstance(inst *instance) {
	s.mu.Lock()
	delete(s.instances, inst.ID)
	s.mu.Unlock()

	s.tearDownInstance(inst, events.ReasonDestroy)
}

// tearDownInstance stops the agent and the backend of inst, then records its
// destroyed event with reason if it was announced. It holds the instance's
// event lock throughout, so the event follows every event of the instance.
func (s *Service) tearDownInstance(inst *instance, reason events.DestroyReason) {
	inst.events.Lock()
	defer inst.events.Unlock()

	s.mu.Lock()
	netAgent := inst.agent
	s.mu.Unlock()

	// Stop the agent first, while the connection it may store through is
	// still up, so in-flight captures flush before the backend servers go away.
	if netAgent != nil {
		err := netAgent.Stop(context.Background())
		if err != nil {
			s.log.Error("Failed to stop agent", "instance", inst.ID, "err", err)
		}
	}

	err := s.backend.Teardown(context.Background(), inst.ID)
	if err != nil {
		s.log.Error("Failed to tear down instance", "instance", inst.ID, "err", err)
	}

	if !inst.announced {
		return
	}

	s.publishEvent(inst, &events.InstanceAction{Action: events.InstanceDestroyed, Description: inst.Description, Reason: reason})
	inst.announced = false
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
