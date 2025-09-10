// Package natsconn provides a flexible connection helper for establishing NATS connections
// with various authentication methods and configuration options.
//
// The package simplifies NATS connection creation by providing a unified configuration
// interface that supports multiple authentication methods including:
//
//   - NATS Context (highest priority)
//   - Credentials file
//   - JWT with seed
//   - NKey authentication
//   - Username/password (basic auth)
//
// It also supports comprehensive TLS configuration options and connection customization.
//
// Example usage:
//
//	cfg := &natsconn.NatsConnConf{
//		NatsServers:        []string{"nats://localhost:4222"},
//		NatsConnectionName: "my-service",
//	}
//
//	nc, err := natsconn.NewNatsConnection(cfg)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer nc.Close()
package natsconn

import (
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/synadia-io/orbit.go/natscontext"
)

// NewNatsConnection creates a new NATS connection using the provided configuration.
//
// The function processes authentication methods in the following priority order:
//  1. NATS Context - If NatsContext is provided, it takes precedence over all other options
//  2. Credentials File - Uses NatsCredentialsFile if provided
//  3. JWT and Seed - Uses NatsUserJWT and NatsUserSeed together
//  4. NKey - Uses NatsUserNkey and NatsUserSeed for NKey authentication
//  5. Username/Password - Uses NatsUser and NatsUserPassword for basic auth
//
// TLS configuration is applied if any TLS options are provided (cert, key, or CA).
//
// Returns a connected nats.Conn on success, or an error if the connection fails.
// The caller is responsible for closing the connection when done.
//
// Example:
//
//	cfg := &NatsConnConf{
//		NatsServers:         []string{"nats://localhost:4222"},
//		NatsConnectionName:  "my-app",
//		NatsCredentialsFile: "./nats.creds",
//	}
//	nc, err := NewNatsConnection(cfg)
//	if err != nil {
//		return err
//	}
//	defer nc.Close()
func NewNatsConnection(cfg *NatsConnConf) (*nats.Conn, error) {
	if cfg == nil {
		return nil, ErrNilNatsConnConfig
	}

	opts := []nats.Option{
		nats.Name(cfg.NatsConnectionName),
	}

	// If a nats context is provided, it takes priority
	if cfg.NatsContext != "" {
		nc, _, err := natscontext.Connect(cfg.NatsContext, opts...)
		if err != nil {
			return nil, errorWithContext(ErrFailToUseNatsContext, err)
		} else {
			return nc, nil
		}
	}

	if len(cfg.NatsServers) == 0 {
		return nil, ErrNoNatsServers
	}

	if cfg.NatsTLSCert != "" && cfg.NatsTLSKey != "" {
		opts = append(opts, nats.ClientCert(cfg.NatsTLSCert, cfg.NatsTLSKey))
	}
	if cfg.NatsTLSCA != "" {
		opts = append(opts, nats.RootCAs(cfg.NatsTLSCA))
	}
	if cfg.NatsTLSFirst {
		opts = append(opts, nats.TLSHandshakeFirst())
	}

	switch {
	case cfg.NatsCredentialsFile != "": // Use credentials file
		opts = append(opts, nats.UserCredentials(cfg.NatsCredentialsFile))
	case cfg.NatsUserSeed != "" && cfg.NatsUserJWT != "": // Use seed + jwt
		opts = append(opts, nats.UserJWTAndSeed(cfg.NatsUserJWT, cfg.NatsUserSeed))
	case cfg.NatsUserNkey != "" && cfg.NatsUserSeed != "": // User nkey
		opts = append(opts, nats.Nkey(cfg.NatsUserNkey, func(nonce []byte) ([]byte, error) {
			kp, err := nkeys.FromSeed([]byte(cfg.NatsUserSeed))
			if err != nil {
				return nil, errorWithContext(ErrFailedToExtractNkeySeed, err)
			}
			return kp.Sign(nonce)
		}))
	case cfg.NatsUser != "" && cfg.NatsUserPassword != "": // Use user + password
		opts = append(opts, nats.UserInfo(cfg.NatsUser, cfg.NatsUserPassword))
	}

	nc, err := nats.Connect(strings.Join(cfg.NatsServers, ","), opts...)
	if err != nil {
		return nil, errorWithContext(ErrNatsConnectionFailed, err)
	}

	return nc, nil
}
