package ntf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/ntf/api"
)

// Default bounds for requests whose context carries no deadline. They match the
// timeouts the testing.TB methods have always used.
const (
	connectTimeout = 10 * time.Second
	createTimeout  = 30 * time.Second
	destroyTimeout = 30 * time.Second
	updateTimeout  = 30 * time.Second
	requestTimeout = 10 * time.Second
)

// Manager is a handle to the management service that returns errors instead of
// failing a test, for use outside of the testing package. Create one with
// Connect. Every call takes a context; a context without a deadline gets the
// same default timeout the matching Client or Instance method uses.
//
// Client and Instance wrap a Manager: their testing.TB methods call the Manager
// and fail the test on error.
type Manager struct {
	address string
	nc      *nats.Conn

	// bareNames is set once the service has refused instance_id on a stop,
	// start, update or reload, so later ones send the bare name straight away.
	bareNames atomic.Bool
}

// Connect connects to the management service at server. Pass extra nats.Option
// values if the management endpoint itself needs auth or TLS.
func Connect(ctx context.Context, server string, opts ...nats.Option) (*Manager, error) {
	u, err := url.Parse(server)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnect, err)
	}

	err = ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnect, err)
	}

	// nats.Connect takes no context, so a deadline on ctx shortens the dial
	// timeout instead.
	timeout := connectTimeout
	deadline, ok := ctx.Deadline()
	if ok {
		timeout = min(timeout, time.Until(deadline))
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("%w: %w", ErrConnect, context.DeadlineExceeded)
	}

	nopts := []nats.Option{
		nats.Timeout(timeout),
		nats.MaxReconnects(-1),
	}

	nc, err := nats.Connect(server, append(nopts, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnect, err)
	}

	return &Manager{nc: nc, address: u.Hostname()}, nil
}

// Close closes the connection to the management service.
func (m *Manager) Close() {
	m.nc.Close()
}

// withDefaultTimeout bounds ctx by timeout unless it already has a deadline.
func withDefaultTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	_, ok := ctx.Deadline()
	if ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// request sends req as JSON to subject and decodes the reply into resp. A nil req
// sends an empty payload. A Nats-Service-Error header on the reply is returned as
// a *serviceError, which matches ErrService, and ErrInstanceNotFound for code
// 404 or ErrServerNameAmbiguous for code 023.
func (m *Manager) request(ctx context.Context, subject string, timeout time.Duration, req any, resp any) error {
	var payload []byte
	if req != nil {
		var err error
		payload, err = json.Marshal(req)
		if err != nil {
			return fmt.Errorf("%w: %s: %w", ErrRequest, subject, err)
		}
	}

	ctx, cancel := withDefaultTimeout(ctx, timeout)
	defer cancel()

	msg, err := m.nc.RequestWithContext(ctx, subject, payload)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrRequest, subject, err)
	}
	e := msg.Header.Get("Nats-Service-Error")
	if e != "" {
		return &serviceError{subject: subject, code: msg.Header.Get("Nats-Service-Error-Code"), desc: e}
	}

	err = json.Unmarshal(msg.Data, resp)
	if err != nil {
		return fmt.Errorf("%w: %s: %q: %w", ErrInvalidResponse, subject, msg.Data, err)
	}
	return nil
}

// serviceError is a refusal from the management service: the subject refused,
// and the code and description the service answered with. It matches ErrService
// under errors.Is, ErrInstanceNotFound for code 404, and ErrServerNameAmbiguous
// for code 023.
type serviceError struct {
	subject string
	code    string
	desc    string
}

// sentinel returns the sentinel the code adds to ErrService, or nil.
func (e *serviceError) sentinel() error {
	switch e.code {
	case "404":
		// The service answers 404 only when the instance is unknown or was
		// destroyed while the request ran.
		return ErrInstanceNotFound
	case "023":
		// A stop, start, update or reload of a name more than one server
		// holds.
		return ErrServerNameAmbiguous
	}
	return nil
}

func (e *serviceError) Error() string {
	sentinel := e.sentinel()
	if sentinel != nil {
		return fmt.Sprintf("%s: %s: %s: %s", ErrService, sentinel, e.subject, e.desc)
	}
	return fmt.Sprintf("%s: %s: %s", ErrService, e.subject, e.desc)
}

func (e *serviceError) Is(target error) bool {
	return target == ErrService || (target != nil && target == e.sentinel())
}

// refusesInstanceID reports whether err is the refusal a service whose request
// schemas predate instance_id answers a stop, start, update or reload carrying
// it with: code 001 and "Invalid request: additional properties 'instance_id'
// not allowed".
func refusesInstanceID(err error) bool {
	var se *serviceError
	if !errors.As(err, &se) || se.code != "001" {
		return false
	}
	return strings.Contains(se.desc, "additional properties") && strings.Contains(se.desc, "'instance_id'")
}

// CreateServer creates a single server. WithName names it. A create option that
// applies to another kind of instance returns ErrInvalidCreateOption.
func (m *Manager) CreateServer(ctx context.Context, js bool, opts ...CreateOption) (*Instance, error) {
	co := resolveCreateOptions(opts)
	err := co.checkKind(kindServer)
	if err != nil {
		return nil, err
	}

	return m.create(ctx, "tester.create.server", api.CreateServerRequest{
		Name:        co.name,
		JetStream:   js,
		Description: co.description,
		Snippets:    co.snippets,
		Template:    co.template,
		TLS:         co.tls,
		Trace:       co.trace,
		Toxics:      co.toxics,
	})
}

// CreateCluster creates a cluster of servers servers. WithClusterName names the
// cluster and WithServerNames its servers; with WithServerNames, servers may be
// zero, and a non-zero count must equal the number of names. A create option
// that applies to another kind of instance returns ErrInvalidCreateOption.
func (m *Manager) CreateCluster(ctx context.Context, servers int, js bool, opts ...CreateOption) (*Instance, error) {
	co := resolveCreateOptions(opts)
	err := co.checkKind(kindCluster)
	if err != nil {
		return nil, err
	}
	if co.serverNames != nil && len(co.serverNames) == 0 {
		return nil, fmt.Errorf("%w: WithServerNames needs at least one name", ErrInvalidCreateOption)
	}

	return m.create(ctx, "tester.create.cluster", api.CreateClusterRequest{
		JetStream:   js,
		Servers:     servers,
		ClusterName: co.clusterName,
		ServerNames: co.serverNames,
		Description: co.description,
		Snippets:    co.snippets,
		Template:    co.template,
		TLS:         co.tls,
		Trace:       co.trace,
		Toxics:      co.toxics,
	})
}

// CreateSuperCluster creates a super-cluster of clusters clusters with servers
// servers each. WithClusters lists the clusters instead, each with its own name,
// size and server names; clusters and servers must then both be zero, or it
// returns ErrInvalidCreateOption. A create option that applies to another kind
// of instance returns ErrInvalidCreateOption too.
func (m *Manager) CreateSuperCluster(ctx context.Context, clusters int, servers int, js bool, opts ...CreateOption) (*Instance, error) {
	co := resolveCreateOptions(opts)
	err := co.checkKind(kindSuperCluster)
	if err != nil {
		return nil, err
	}
	if co.clusterList != nil && len(co.clusterList) == 0 {
		return nil, fmt.Errorf("%w: WithClusters needs at least one cluster", ErrInvalidCreateOption)
	}
	if co.clusterList != nil && (clusters != 0 || servers != 0) {
		return nil, fmt.Errorf("%w: with WithClusters the clusters and servers arguments must be zero, got %d and %d", ErrInvalidCreateOption, clusters, servers)
	}

	return m.create(ctx, "tester.create.super-cluster", api.CreateSuperClusterRequest{
		JetStream:   js,
		Clusters:    clusters,
		Servers:     servers,
		ClusterList: co.clusterList,
		Description: co.description,
		Snippets:    co.snippets,
		Template:    co.template,
		TLS:         co.tls,
		Trace:       co.trace,
		Toxics:      co.toxics,
	})
}

func (m *Manager) create(ctx context.Context, subject string, req any) (*Instance, error) {
	resp := api.CreateResponse{}
	err := m.request(ctx, subject, createTimeout, req, &resp)
	if err != nil {
		return nil, err
	}

	for _, srv := range resp.Servers {
		srv.URL = fmt.Sprintf("nats://%s:%d", m.address, srv.Port)
		tp, ok := srv.Ports["trace"]
		if ok && tp != 0 {
			srv.TraceURL = fmt.Sprintf("nats://%s:%d", m.address, tp)
		}
	}

	return &Instance{
		ID:          resp.ID,
		Description: resp.Description,
		Kind:        resp.Kind,
		Servers:     resp.Servers,
		TLS:         resp.TLS,
		Links:       resp.Links,
		m:           m,
	}, nil
}

// List returns a lightweight summary of every instance currently held by the
// management service.
func (m *Manager) List(ctx context.Context) (*api.ListResponse, error) {
	resp := api.ListResponse{}
	err := m.request(ctx, "tester.list", requestTimeout, nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// Reset shuts down and removes all servers across every instance.
func (m *Manager) Reset(ctx context.Context) (*api.ResetResponse, error) {
	resp := api.ResetResponse{}
	err := m.request(ctx, "tester.reset", requestTimeout, nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// Status returns the status of every instance managed by the service.
func (m *Manager) Status(ctx context.Context) (*api.StatusResponse, error) {
	resp := api.StatusResponse{}
	err := m.request(ctx, "tester.status", requestTimeout, nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// InstanceStatus returns the status of the instance with the given ID, or
// ErrInstanceNotFound when the service holds no such instance.
func (m *Manager) InstanceStatus(ctx context.Context, instanceID string) (*api.InstanceStatus, error) {
	resp := api.StatusResponse{}
	err := m.request(ctx, "tester.status", requestTimeout, api.StatusRequest{InstanceID: instanceID}, &resp)
	if err != nil {
		return nil, err
	}
	if len(resp.Instances) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	return &resp.Instances[0], nil
}

// Destroy tears down the instance with the given ID: shuts down its servers and
// removes its storage dir.
func (m *Manager) Destroy(ctx context.Context, instanceID string) (*api.DestroyResponse, error) {
	resp := api.DestroyResponse{}
	err := m.request(ctx, "tester.destroy", destroyTimeout, api.DestroyRequest{InstanceID: instanceID}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// StopServer stops the managed server with the given name, wherever it is. A
// service where more than one server holds the name refuses it with
// ErrServerNameAmbiguous; use StopInstanceServer to say which instance's server
// is meant.
func (m *Manager) StopServer(ctx context.Context, name string) (*api.StopServerResponse, error) {
	return m.StopInstanceServer(ctx, "", name)
}

// StopInstanceServer stops the managed server named name in the instance with
// ID instanceID.
//
// The request carries the instance ID, so only that instance is searched: an
// unknown instance returns ErrInstanceNotFound, and a name the instance does
// not hold returns ErrService. A service from before instance IDs were
// accepted either ignores the ID, or refuses it and is asked again with the bare
// name, which this Manager then sends from that point on. Either way such a
// service finds the name among all its servers, so name should be one of the
// instance's own. An empty instanceID behaves as StopServer.
func (m *Manager) StopInstanceServer(ctx context.Context, instanceID string, name string) (*api.StopServerResponse, error) {
	if name == "" {
		return nil, ErrServerRequired
	}

	resp := api.StopServerResponse{}
	err := m.instanceServerRequest(ctx, "tester.stop.server", requestTimeout, instanceID, func(id string) any {
		return api.StopServerRequest{Name: name, InstanceID: id}
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// StartServer starts the previously stopped managed server with the given name,
// wherever it is. A service where more than one server holds the name refuses
// it with ErrServerNameAmbiguous; use StartInstanceServer to say which
// instance's server is meant.
func (m *Manager) StartServer(ctx context.Context, name string) (*api.StartServerResponse, error) {
	return m.StartInstanceServer(ctx, "", name)
}

// StartInstanceServer starts the previously stopped managed server named name
// in the instance with ID instanceID.
//
// The request carries the instance ID, so only that instance is searched: an
// unknown instance returns ErrInstanceNotFound, and a name the instance does
// not hold returns ErrService. A service from before instance IDs were
// accepted either ignores the ID, or refuses it and is asked again with the bare
// name, which this Manager then sends from that point on. Either way such a
// service finds the name among all its servers, so name should be one of the
// instance's own. An empty instanceID behaves as StartServer.
func (m *Manager) StartInstanceServer(ctx context.Context, instanceID string, name string) (*api.StartServerResponse, error) {
	if name == "" {
		return nil, ErrServerRequired
	}

	resp := api.StartServerResponse{}
	err := m.instanceServerRequest(ctx, "tester.start.server", requestTimeout, instanceID, func(id string) any {
		return api.StartServerRequest{Name: name, InstanceID: id}
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// UpdateServer re-renders the config of the managed server with the given name,
// wherever it is, from opts and writes it to disk without reloading it. See
// Instance.UpdateServer for the full-replace semantics. A service where more
// than one server holds the name refuses it with ErrServerNameAmbiguous; use
// UpdateInstanceServer to say which instance's server is meant.
func (m *Manager) UpdateServer(ctx context.Context, name string, opts ...UpdateOption) (*api.UpdateServerResponse, error) {
	return m.UpdateInstanceServer(ctx, "", name, opts...)
}

// UpdateInstanceServer re-renders the config of the managed server named name
// in the instance with ID instanceID from opts and writes it to disk without
// reloading it. See Instance.UpdateServer for the full-replace semantics.
//
// The request carries the instance ID, so only that instance is searched: an
// unknown instance returns ErrInstanceNotFound, and a name the instance does
// not hold returns ErrService. A service from before instance IDs were
// accepted either ignores the ID, or refuses it and is asked again with the bare
// name, which this Manager then sends from that point on. Either way such a
// service finds the name among all its servers, so name should be one of the
// instance's own. An empty instanceID behaves as UpdateServer.
func (m *Manager) UpdateInstanceServer(ctx context.Context, instanceID string, name string, opts ...UpdateOption) (*api.UpdateServerResponse, error) {
	if name == "" {
		return nil, ErrServerRequired
	}

	uo := resolveUpdateOptions(opts)
	resp := api.UpdateServerResponse{}
	err := m.instanceServerRequest(ctx, "tester.update.server", updateTimeout, instanceID, func(id string) any {
		return api.UpdateServerRequest{
			Name:       name,
			InstanceID: id,
			Snippets:   uo.snippets,
			Template:   uo.template,
			TLSTimeout: uo.tlsTimeout,
		}
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// ReloadServer signals the running managed server with the given name, wherever
// it is, to re-read its on-disk config. A service where more than one server
// holds the name refuses it with ErrServerNameAmbiguous; use
// ReloadInstanceServer to say which instance's server is meant.
func (m *Manager) ReloadServer(ctx context.Context, name string) (*api.ReloadServerResponse, error) {
	return m.ReloadInstanceServer(ctx, "", name)
}

// ReloadInstanceServer signals the running managed server named name in the
// instance with ID instanceID to re-read its on-disk config.
//
// The request carries the instance ID, so only that instance is searched: an
// unknown instance returns ErrInstanceNotFound, and a name the instance does
// not hold returns ErrService. A service from before instance IDs were
// accepted either ignores the ID, or refuses it and is asked again with the bare
// name, which this Manager then sends from that point on. Either way such a
// service finds the name among all its servers, so name should be one of the
// instance's own. An empty instanceID behaves as ReloadServer.
func (m *Manager) ReloadInstanceServer(ctx context.Context, instanceID string, name string) (*api.ReloadServerResponse, error) {
	if name == "" {
		return nil, ErrServerRequired
	}

	resp := api.ReloadServerResponse{}
	err := m.instanceServerRequest(ctx, "tester.reload.server", updateTimeout, instanceID, func(id string) any {
		return api.ReloadServerRequest{Name: name, InstanceID: id}
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// instanceServerRequest sends a stop, start, update or reload of the server
// named in the request build makes; build is given the instance ID to put on
// the request, or "" for a bare name.
//
// An empty instanceID sends the bare name, which every service searches for
// among all its servers. Otherwise the request carries instanceID, and what
// happens depends on the service's generation:
//
//   - services from before request schemas ignore instance_id and search all
//     servers by name; names were unique across instances then, so the one
//     request does the work.
//   - services with request schemas from before instance_id refuse the field
//     (see refusesInstanceID). The request is sent again with the bare name,
//     and this Manager sends the bare name from then on.
//   - services with instance_id search only that instance: an unknown instance
//     is refused with 404 and a name it does not hold with 001.
//
// A bare name more than one server holds is refused with 023 and returned as
// ErrServerNameAmbiguous, without a further retry.
func (m *Manager) instanceServerRequest(ctx context.Context, subject string, timeout time.Duration, instanceID string, build func(instanceID string) any, resp any) error {
	if instanceID == "" || m.bareNames.Load() {
		return m.request(ctx, subject, timeout, build(""), resp)
	}

	err := m.request(ctx, subject, timeout, build(instanceID), resp)
	if !refusesInstanceID(err) {
		return err
	}

	m.bareNames.Store(true)
	return m.request(ctx, subject, timeout, build(""), resp)
}

// TraceStore returns the management server's TRACES object store, where traces
// captured via WithTraceCapture are stored.
func (m *Manager) TraceStore(ctx context.Context) (jetstream.ObjectStore, error) {
	ctx, cancel := withDefaultTimeout(ctx, requestTimeout)
	defer cancel()

	js, err := jetstream.New(m.nc)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTraceStore, err)
	}
	store, err := js.ObjectStore(ctx, "TRACES")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTraceStore, err)
	}
	return store, nil
}
