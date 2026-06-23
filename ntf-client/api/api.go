package api

import "time"

// TLSMode selects the verification posture for managed TLS. Absence of a
// TLSOptions value disables TLS entirely; the modes only apply when TLS is
// requested.
type TLSMode string

const (
	// TLSModeServer is one-way TLS: the server presents a cert signed by the
	// generated CA but does not require a client cert.
	TLSModeServer TLSMode = "server"
	// TLSModeMutual requires the client to present the issued client cert.
	TLSModeMutual TLSMode = "mutual"
)

// TLSOptions opts a managed instance into generated TLS on its client ports.
// Gateways and routes remain plaintext.
type TLSOptions struct {
	// Mode defaults to TLSModeMutual when empty.
	Mode TLSMode `json:"mode,omitempty"`
	// SANs are the Subject Alternative Names to embed in the server leaf.
	// Strings that parse as IPs become IP SANs; everything else is treated
	// as a DNS name. Empty means ["localhost","127.0.0.1","::1"].
	SANs []string `json:"sans,omitempty"`
	// HandshakeFirst makes the server perform the TLS handshake before sending
	// the INFO protocol. Clients must dial with the matching handshake-first
	// option; the convenience helpers wire it automatically.
	HandshakeFirst bool `json:"handshake_first,omitempty"`
	// Timeout is the TLS handshake timeout (the nats-server tls{} `timeout`)
	// in seconds; fractional values are honored (0.0001 = 100us). Zero uses the
	// managed default of 2 seconds. The Go client helpers set this from a
	// time.Duration.
	Timeout float64 `json:"timeout,omitempty"`
}

// TLSMaterial carries the cert material a caller needs to dial a managed
// instance over TLS. ClientCertPEM/ClientKeyPEM are populated only for
// TLSModeMutual; the server's private key never leaves the service.
type TLSMaterial struct {
	CAPEM         string `json:"ca_pem"`
	ClientCertPEM string `json:"client_cert_pem,omitempty"`
	ClientKeyPEM  string `json:"client_key_pem,omitempty"`
}

type CreateServerRequest struct {
	JetStream   bool              `json:"jetstream"`
	Description string            `json:"description,omitempty"`
	Snippets    map[string]string `json:"snippets,omitempty"`
	Template    string            `json:"template,omitempty"`
	TLS         *TLSOptions       `json:"tls,omitempty"`
	// Trace opts the server into a capture proxy on a random port; every connection
	// that reaches it has its trace stored in the TRACES object store.
	Trace bool `json:"trace,omitempty"`
}

type CreateClusterRequest struct {
	Servers     int               `json:"servers"`
	JetStream   bool              `json:"jetstream"`
	Description string            `json:"description,omitempty"`
	Snippets    map[string]string `json:"snippets,omitempty"`
	Template    string            `json:"template,omitempty"`
	TLS         *TLSOptions       `json:"tls,omitempty"`
	// Trace fronts the first node of the cluster with a single capture proxy that
	// stores every connection's trace.
	Trace bool `json:"trace,omitempty"`
}

type CreateSuperClusterRequest struct {
	Servers     int               `json:"servers"`
	Clusters    int               `json:"clusters"`
	JetStream   bool              `json:"jetstream"`
	Description string            `json:"description,omitempty"`
	Snippets    map[string]string `json:"snippets,omitempty"`
	Template    string            `json:"template,omitempty"`
	TLS         *TLSOptions       `json:"tls,omitempty"`
	// Trace fronts the first node of the first cluster with a single capture proxy
	// that stores every connection's trace.
	Trace bool `json:"trace,omitempty"`
}

type ManagedServer struct {
	Name    string         `json:"name"`
	Cluster string         `json:"cluster"`
	Port    int            `json:"port"`
	Ports   map[string]int `json:"ports,omitempty"`
	URL     string         `json:"url,omitempty"`
	Running bool           `json:"running"`
	// TraceURL is the capture-proxy URL for a server created with trace capture
	// enabled. Set client-side from Ports["trace"]; empty when not traced.
	TraceURL string `json:"trace_url,omitempty"`
}

type CreateResponse struct {
	ID          string           `json:"id"`
	Description string           `json:"description,omitempty"`
	Kind        string           `json:"kind"`
	Servers     []*ManagedServer `json:"servers"`
	TLS         *TLSMaterial     `json:"tls,omitempty"`
}

type DestroyRequest struct {
	InstanceID string `json:"instance_id"`
}

type DestroyResponse struct {
	Destroyed bool `json:"destroyed"`
}

type InstanceSummary struct {
	ID          string    `json:"id"`
	Description string    `json:"description,omitempty"`
	Kind        string    `json:"kind"`
	Cluster     string    `json:"cluster,omitempty"`
	Servers     int       `json:"servers"`
	Created     time.Time `json:"created"`
}

type ListResponse struct {
	Instances []InstanceSummary `json:"instances"`
}

type ResetResponse struct {
	Shutdown bool `json:"shutdown"`
}

type StartServerRequest struct {
	Name string `json:"name"`
}

type StopServerRequest struct {
	Name string `json:"name"`
}

type StopServerResponse struct {
	Shutdown bool `json:"shutdown"`
}

type StartServerResponse struct {
	Started bool `json:"started"`
}

// ServerStateResult reports the post-operation state of one managed server in an
// instance-level stop or start. Error is empty on success; a non-empty Error means
// that node could not be transitioned (the operation is best-effort per node).
type ServerStateResult struct {
	Name    string `json:"name"`
	Running bool   `json:"running"`
	Error   string `json:"error,omitempty"`
}

// StopInstanceRequest stops every server in an instance while keeping its config and
// storage on disk so StartInstance can revive it later.
type StopInstanceRequest struct {
	InstanceID string `json:"instance_id"`
}

type StopInstanceResponse struct {
	Servers []ServerStateResult `json:"servers"`
}

// StartInstanceRequest revives a previously stopped instance, restarting each server
// from its persisted config.
type StartInstanceRequest struct {
	InstanceID string `json:"instance_id"`
}

type StartInstanceResponse struct {
	Servers []ServerStateResult `json:"servers"`
}

type StatusRequest struct {
	InstanceID string `json:"instance_id,omitempty"`
}

type InstanceStatus struct {
	ID          string          `json:"id"`
	Description string          `json:"description,omitempty"`
	Kind        string          `json:"kind"`
	Servers     []ManagedServer `json:"servers"`
}

type StatusResponse struct {
	Instances []InstanceStatus `json:"instances"`
}

type UpdateServerRequest struct {
	Name     string            `json:"name"`
	Snippets map[string]string `json:"snippets,omitempty"`
	Template string            `json:"template,omitempty"`
	// TLSTimeout, when non-nil, sets the managed TLS handshake timeout (seconds)
	// for a server created with generated TLS. Absent means no change. Apply it
	// with a subsequent reload.
	TLSTimeout *float64 `json:"tls_timeout,omitempty"`
}

type UpdateServerResponse struct {
	Updated bool `json:"updated"`
}

type ReloadServerRequest struct {
	Name string `json:"name"`
}

type ReloadServerResponse struct {
	Reloaded bool `json:"reloaded"`
}
