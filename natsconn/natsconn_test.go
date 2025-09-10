package natsconn

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
)

func TestNewNatsConnection_NilConfig(t *testing.T) {
	nc, err := NewNatsConnection(nil)
	if err != ErrNilNatsConnConfig {
		t.Fatalf("Expected ErrNilNatsConnConfig, got %v", err)
	}
	if nc != nil {
		t.Fatal("Expected nil connection for nil config")
	}
}

func TestNewNatsConnection_NoServers(t *testing.T) {
	cfg := &NatsConnConf{
		NatsConnectionName: "test-connection",
	}
	nc, err := NewNatsConnection(cfg)
	if err != ErrNoNatsServers {
		t.Fatalf("Expected ErrNoNatsServers, got %v", err)
	}
	if nc != nil {
		t.Fatal("Expected nil connection when no servers provided")
	}
}

func TestNewNatsConnection_Basic(t *testing.T) {
	s := runBasicServer(t)
	defer s.Shutdown()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("Server failed to start")
	}

	cfg := &NatsConnConf{
		NatsServers:        []string{s.ClientURL()},
		NatsConnectionName: "test-basic",
	}

	nc, err := NewNatsConnection(cfg)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	if !nc.IsConnected() {
		t.Fatal("Expected to be connected")
	}
}

func TestNewNatsConnection_BasicAuth(t *testing.T) {
	user := "testuser"
	pass := "testpass"

	s := runServerWithBasicAuth(t, user, pass)
	defer s.Shutdown()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("Server failed to start")
	}

	cfg := &NatsConnConf{
		NatsServers:        []string{s.ClientURL()},
		NatsConnectionName: "test-basic-auth",
		NatsUser:           user,
		NatsUserPassword:   pass,
	}

	nc, err := NewNatsConnection(cfg)
	if err != nil {
		t.Fatalf("Failed to connect with correct credentials: %v", err)
	}
	defer nc.Close()

	if !nc.IsConnected() {
		t.Fatal("Expected to be connected")
	}

	// Test failed auth
	badCfg := &NatsConnConf{
		NatsServers:        []string{s.ClientURL()},
		NatsConnectionName: "test-bad-auth",
		NatsUser:           "wronguser",
		NatsUserPassword:   "wrongpass",
	}

	nc2, err := NewNatsConnection(badCfg)
	if err == nil {
		nc2.Close()
		t.Fatal("Expected authentication error")
	}
}

func TestNewNatsConnection_NKey(t *testing.T) {
	s, _, seed := runServerWithNKey(t)
	defer s.Shutdown()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("Server failed to start")
	}

	kp, err := nkeys.FromSeed([]byte(seed))
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}

	cfg := &NatsConnConf{
		NatsServers:        []string{s.ClientURL()},
		NatsConnectionName: "test-nkey",
		NatsUserNkey:       pub,
		NatsUserSeed:       seed,
	}

	nc, err := NewNatsConnection(cfg)
	if err != nil {
		t.Fatalf("Failed to connect with NKey: %v", err)
	}
	defer nc.Close()

	if !nc.IsConnected() {
		t.Fatal("Expected to be connected")
	}
}

func TestNewNatsConnection_CredentialsFile(t *testing.T) {
	s, jwt, seed := runServerWithJWT(t)
	defer s.Shutdown()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("Server failed to start")
	}

	credsContent := fmt.Sprintf(`-----BEGIN NATS USER JWT-----
%s
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
%s
------END USER NKEY SEED------

*************************************************************
`, jwt, seed)

	tmpDir := t.TempDir()
	err := os.WriteFile(filepath.Join(tmpDir, "user.creds"), []byte(credsContent), 0600)
	if err != nil {
		t.Fatalf("Failed to write creds file: %v", err)
	}

	cfg := &NatsConnConf{
		NatsServers:         []string{s.ClientURL()},
		NatsConnectionName:  "test-creds",
		NatsCredentialsFile: filepath.Join(tmpDir, "user.creds"),
	}

	nc, err := NewNatsConnection(cfg)
	if err == nil {
		nc.Close()
		t.Fatal("Expected authentication error")
	}
}

func TestNewNatsConnection_TLSFirst(t *testing.T) {
	s, certFile, keyFile, caFile := runServerWithTLSHandshakeFirst(t)
	defer s.Shutdown()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("Server failed to start")
	}

	// Get the TLS URL
	tlsURL := s.ClientURL()
	// Replace nats:// with tls://
	if len(tlsURL) > 7 && tlsURL[:7] == "nats://" {
		tlsURL = "tls://" + tlsURL[7:]
	}

	cfg := &NatsConnConf{
		NatsServers:        []string{tlsURL},
		NatsConnectionName: "test-tls-first",
		NatsTLSCert:        certFile,
		NatsTLSKey:         keyFile,
		NatsTLSCA:          caFile,
		NatsTLSFirst:       true,
	}

	nc, err := NewNatsConnection(cfg)
	if err != nil {
		t.Fatalf("Failed to connect with TLS first: %v", err)
	}
	defer nc.Close()

	if !nc.IsConnected() {
		t.Fatal("Expected to be connected")
	}
}
