package natssysclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/natsext"
)

const (
	// DefaultRequestTimeout is the default timeout for requests, used when the provided context does not have a deadline.
	DefaultRequestTimeout = 10 * time.Second
	DefaultStall          = 300 * time.Millisecond
)

const (
	srvVarzSubj    = "$SYS.REQ.SERVER.%s.VARZ"
	srvStatszSubj  = "$SYS.REQ.SERVER.%s.STATSZ"
	srvConnzSubj   = "$SYS.REQ.SERVER.%s.CONNZ"
	srvSubszSubj   = "$SYS.REQ.SERVER.%s.SUBSZ"
	srvHealthzSubj = "$SYS.REQ.SERVER.%s.HEALTHZ"
	srvJszSubj     = "$SYS.REQ.SERVER.%s.JSZ"
)

var (
	ErrValidation      = errors.New("validation error")
	ErrInvalidServerID = errors.New("sever with given ID does not exist")
)

// System can be used to request monitoring data from the server.
type System struct {
	nc   *nats.Conn
	opts *sysClientOpts
}

// SysClientOpt is a functional option for configuring the System client.
type SysClientOpt func(*sysClientOpts) error

type sysClientOpts struct {
	multiRequestInterval time.Duration
	serverCount          int
}

// StallTimer sets the interval for subsequent responses when pinging the cluster.
func StallTimer(initialTimeout, interval time.Duration) SysClientOpt {
	return func(opts *sysClientOpts) error {
		if interval <= 0 {
			return fmt.Errorf("%w: interval has to be greater than 0", ErrValidation)
		}
		if initialTimeout <= 0 {
			initialTimeout = interval
		}
		opts.multiRequestInterval = interval
		return nil
	}
}

// ServerCount sets the maximum number of servers to wait for the response from.
func ServerCount(count int) SysClientOpt {
	return func(opts *sysClientOpts) error {
		if count <= 0 {
			return fmt.Errorf("%w: server count has to be greater than 0", ErrValidation)
		}
		opts.serverCount = count
		return nil
	}
}

// ServerInfo identifies remote servers.
type ServerInfo struct {
	Name      string    `json:"name"`
	Host      string    `json:"host"`
	ID        string    `json:"id"`
	Cluster   string    `json:"cluster,omitempty"`
	Domain    string    `json:"domain,omitempty"`
	Version   string    `json:"ver"`
	Tags      []string  `json:"tags,omitempty"`
	Seq       uint64    `json:"seq"`
	JetStream bool      `json:"jetstream"`
	Time      time.Time `json:"time"`
}

// NewSysClient creates a new System client.
func NewSysClient(nc *nats.Conn, opts ...SysClientOpt) (*System, error) {
	sysOpts := &sysClientOpts{
		multiRequestInterval: DefaultStall,
		serverCount:          -1,
	}
	for _, opt := range opts {
		if err := opt(sysOpts); err != nil {
			return nil, err
		}
	}
	return &System{
		nc:   nc,
		opts: sysOpts,
	}, nil
}

// APIError is a generic error response from nats-server.
type APIError struct {
	Code        int    `json:"code"`
	ErrCode     uint16 `json:"err_code,omitempty"`
	Description string `json:"description,omitempty"`
}

func (s *System) pingServers(ctx context.Context, subject string, data []byte) ([]*nats.Msg, error) {
	if subject == "" {
		return nil, fmt.Errorf("%w: subject cannot be empty", ErrValidation)
	}

	opts := []natsext.RequestManyOpt{}
	if s.opts.serverCount > 0 {
		opts = append(opts, natsext.RequestManyMaxMessages(s.opts.serverCount))
	}
	if s.opts.multiRequestInterval > 0 {
		opts = append(opts, natsext.RequestManyStall(s.opts.multiRequestInterval))
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultRequestTimeout)
		defer cancel()
	}
	iter, err := natsext.RequestMany(ctx, s.nc, subject, data, opts...)
	if err != nil {
		return nil, err
	}
	msgs := make([]*nats.Msg, 0)
	for msg, err := range iter {
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}
