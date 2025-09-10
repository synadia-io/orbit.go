package natsconn

import "time"

// NatsConnConf represents the configuration options for establishing a NATS connection.
// It supports multiple authentication methods and connection customization options.
//
// Authentication methods are evaluated in priority order:
//  1. NatsContext - If provided, uses NATS context configuration
//  2. NatsCredentialsFile - Uses a credentials file
//  3. NatsUserJWT + NatsUserSeed - JWT-based authentication
//  4. NatsUserNkey + NatsUserSeed - NKey-based authentication
//  5. NatsUser + NatsUserPassword - Basic username/password authentication
//
// Example:
//
//	cfg := &NatsConnConf{
//		NatsServers:        []string{"nats://localhost:4222"},
//		NatsConnectionName: "my-service",
//		NatsTimeout:        5 * time.Second,
//	}
type NatsConnConf struct {
	// NatsContext specifies a NATS context to use for connection.
	// If provided, it takes priority over all other configuration options.
	NatsContext string `json:"nats_context,omitempty"`
	// NatsServers is a list of NATS server URLs to connect to.
	// Example: []string{"nats://localhost:4222", "nats://localhost:4223"}
	NatsServers []string `json:"nats_servers,omitempty"`
	// NatsUserNkey is the user's public NKey for NKey-based authentication.
	// Must be used together with NatsUserSeed.
	NatsUserNkey string `json:"nats_user_nkey,omitempty"`
	// NatsUserSeed is the seed for NKey or JWT-based authentication.
	// Used with either NatsUserNkey or NatsUserJWT.
	NatsUserSeed string `json:"nats_user_seed,omitempty"`
	// NatsUserJWT is the user JWT for JWT-based authentication.
	// Must be used together with NatsUserSeed.
	NatsUserJWT string `json:"nats_user_jwt,omitempty"`
	// NatsUser is the username for basic authentication.
	// Must be used together with NatsUserPassword.
	NatsUser string `json:"nats_user,omitempty"`
	// NatsUserPassword is the password for basic authentication.
	// Must be used together with NatsUser.
	NatsUserPassword string `json:"nats_user_password,omitempty"`
	// NatsJSDomain specifies the JetStream domain to use.
	NatsJSDomain string `json:"nats_js_domain,omitempty"`
	// NatsConnectionName is a human-readable name for the connection.
	// This appears in NATS server monitoring and logs.
	NatsConnectionName string `json:"nats_connection_name,omitempty"`
	// NatsCredentialsFile is the path to a NATS credentials file.
	// Takes precedence over other authentication methods except NatsContext.
	NatsCredentialsFile string `json:"nats_credentials_file,omitempty"`
	// NatsTimeout is the connection timeout duration.
	// If not set, uses the NATS client library default.
	NatsTimeout time.Duration `json:"nats_timeout,omitempty"`
	// NatsTLSCert is the path to the client TLS certificate file.
	// Must be used together with NatsTLSKey.
	NatsTLSCert string `json:"nats_tls_cert,omitempty"`
	// NatsTLSKey is the path to the client TLS key file.
	// Must be used together with NatsTLSCert.
	NatsTLSKey string `json:"nats_tls_key,omitempty"`
	// NatsTLSCA is the path to the CA certificate file for verifying the server.
	NatsTLSCA string `json:"nats_tlsca,omitempty"`
	// NatsTLSFirst enables TLS handshake first mode.
	// When true, performs TLS handshake before sending NATS protocol.
	NatsTLSFirst bool `json:"nats_tls_first,omitempty"`
}
