package ntf

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
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
// ErrService.
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
		// The service answers 404 only when the instance is unknown or was
		// destroyed while the request ran.
		if msg.Header.Get("Nats-Service-Error-Code") == "404" {
			return fmt.Errorf("%w: %w: %s: %s", ErrService, ErrInstanceNotFound, subject, e)
		}
		return fmt.Errorf("%w: %s: %s", ErrService, subject, e)
	}

	err = json.Unmarshal(msg.Data, resp)
	if err != nil {
		return fmt.Errorf("%w: %s: %q: %w", ErrInvalidResponse, subject, msg.Data, err)
	}
	return nil
}

// CreateServer creates a single server.
func (m *Manager) CreateServer(ctx context.Context, js bool, opts ...CreateOption) (*Instance, error) {
	co := resolveCreateOptions(opts)
	return m.create(ctx, "tester.create.server", api.CreateServerRequest{
		JetStream:   js,
		Description: co.description,
		Snippets:    co.snippets,
		Template:    co.template,
		TLS:         co.tls,
		Trace:       co.trace,
	})
}

// CreateCluster creates a cluster of servers.
func (m *Manager) CreateCluster(ctx context.Context, servers int, js bool, opts ...CreateOption) (*Instance, error) {
	co := resolveCreateOptions(opts)
	return m.create(ctx, "tester.create.cluster", api.CreateClusterRequest{
		JetStream:   js,
		Servers:     servers,
		Description: co.description,
		Snippets:    co.snippets,
		Template:    co.template,
		TLS:         co.tls,
		Trace:       co.trace,
	})
}

// CreateSuperCluster creates a super-cluster of clusters clusters with servers
// servers each.
func (m *Manager) CreateSuperCluster(ctx context.Context, clusters int, servers int, js bool, opts ...CreateOption) (*Instance, error) {
	co := resolveCreateOptions(opts)
	return m.create(ctx, "tester.create.super-cluster", api.CreateSuperClusterRequest{
		JetStream:   js,
		Clusters:    clusters,
		Servers:     servers,
		Description: co.description,
		Snippets:    co.snippets,
		Template:    co.template,
		TLS:         co.tls,
		Trace:       co.trace,
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

// StopServer stops the managed server with the given name. Server names are
// unique across instances.
func (m *Manager) StopServer(ctx context.Context, name string) (*api.StopServerResponse, error) {
	if name == "" {
		return nil, ErrServerRequired
	}

	resp := api.StopServerResponse{}
	err := m.request(ctx, "tester.stop.server", requestTimeout, api.StopServerRequest{Name: name}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// StartServer starts the previously stopped managed server with the given name.
func (m *Manager) StartServer(ctx context.Context, name string) (*api.StartServerResponse, error) {
	if name == "" {
		return nil, ErrServerRequired
	}

	resp := api.StartServerResponse{}
	err := m.request(ctx, "tester.start.server", requestTimeout, api.StartServerRequest{Name: name}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// UpdateServer re-renders the config of the managed server with the given name
// from opts and writes it to disk without reloading it. See
// Instance.UpdateServer for the full-replace semantics.
func (m *Manager) UpdateServer(ctx context.Context, name string, opts ...UpdateOption) (*api.UpdateServerResponse, error) {
	if name == "" {
		return nil, ErrServerRequired
	}

	uo := resolveUpdateOptions(opts)
	req := api.UpdateServerRequest{
		Name:       name,
		Snippets:   uo.snippets,
		Template:   uo.template,
		TLSTimeout: uo.tlsTimeout,
	}

	resp := api.UpdateServerResponse{}
	err := m.request(ctx, "tester.update.server", updateTimeout, req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// ReloadServer signals the running managed server with the given name to
// re-read its on-disk config.
func (m *Manager) ReloadServer(ctx context.Context, name string) (*api.ReloadServerResponse, error) {
	if name == "" {
		return nil, ErrServerRequired
	}

	resp := api.ReloadServerResponse{}
	err := m.request(ctx, "tester.reload.server", updateTimeout, api.ReloadServerRequest{Name: name}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
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
