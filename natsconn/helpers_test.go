package natsconn

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nkeys"
)

// runBasicServer starts a basic NATS server with no authentication
func runBasicServer(t *testing.T) *server.Server {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1 // Use random port
	s := natsserver.RunServer(&opts)
	return s
}

// runServerWithBasicAuth starts a NATS server with username/password authentication
func runServerWithBasicAuth(t *testing.T, user, password string) *server.Server {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.Username = user
	opts.Password = password
	s := natsserver.RunServer(&opts)
	return s
}

// runServerWithNKey starts a NATS server configured for NKey authentication
func runServerWithNKey(t *testing.T) (*server.Server, nkeys.KeyPair, string) {
	t.Helper()

	// Create a user keypair
	ukp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}

	pub, err := ukp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}

	seed, err := ukp.Seed()
	if err != nil {
		t.Fatal(err)
	}

	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.Nkeys = []*server.NkeyUser{
		{
			Nkey: pub,
		},
	}

	s := natsserver.RunServer(&opts)
	return s, ukp, string(seed)
}

// runServerWithJWT starts a NATS server that accepts JWT authentication
// This is a simplified version for testing - real JWT auth requires more setup
func runServerWithJWT(t *testing.T) (*server.Server, string, string) {
	t.Helper()

	// For testing JWT, we need to set up operator mode
	// This is a simplified setup
	okp, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatal(err)
	}

	opub, err := okp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}

	// Create account
	akp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}

	aPub, err := akp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}

	// Create account claims
	accountClaims := jwt.NewAccountClaims(aPub)
	accountClaims.Name = "test-account"

	// Sign account JWT with operator key
	_, err = accountClaims.Encode(okp)
	if err != nil {
		t.Fatal(err)
	}

	// Create user
	ukp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}

	seed, err := ukp.Seed()
	if err != nil {
		t.Fatal(err)
	}

	uPub, err := ukp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}

	// Create user claims
	userClaims := jwt.NewUserClaims(uPub)
	userClaims.Name = "test-user"
	userClaims.IssuerAccount = aPub

	// Sign user JWT with account key
	userJWT, err := userClaims.Encode(akp)
	if err != nil {
		t.Fatal(err)
	}

	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.TrustedKeys = []string{opub}

	s := natsserver.RunServer(&opts)
	return s, userJWT, string(seed)
}

// runServerWithTLS starts a NATS server with TLS enabled
func runServerWithTLS(t *testing.T) (*server.Server, string, string, string) {
	t.Helper()

	// Create temporary directory for certificates
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "server-cert.pem")
	keyFile := filepath.Join(tmpDir, "server-key.pem")
	caFile := filepath.Join(tmpDir, "ca.pem")

	// Generate a self-signed certificate for testing
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test Org"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{""},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:              []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}

	// Write cert to file
	certOut, err := os.Create(certFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatal(err)
	}
	certOut.Close()

	// Write key to file
	keyOut, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	privKeyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privKeyBytes}); err != nil {
		t.Fatal(err)
	}
	keyOut.Close()

	// Copy cert to CA file (self-signed)
	if err := os.WriteFile(caFile, []byte(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})), 0644); err != nil {
		t.Fatal(err)
	}

	// Load the generated certificate
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to load generated cert and key: %v", err)
	}

	// Create CA pool
	caCertPool := x509.NewCertPool()
	caCertPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	if !caCertPool.AppendCertsFromPEM(caCertPEM) {
		t.Fatal("Failed to append CA cert to pool")
	}

	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		MinVersion:   tls.VersionTLS12,
	}

	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.TLSConfig = tc

	s := natsserver.RunServer(&opts)
	return s, certFile, keyFile, caFile
}

// runServerWithTLSHandshakeFirst starts a NATS server with TLS handshake first enabled
func runServerWithTLSHandshakeFirst(t *testing.T) (*server.Server, string, string, string) {
	t.Helper()

	// Create temporary directory for certificates
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "server-cert.pem")
	keyFile := filepath.Join(tmpDir, "server-key.pem")
	caFile := filepath.Join(tmpDir, "ca.pem")

	// Generate a self-signed certificate for testing
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test Org"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{""},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:              []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}

	// Write cert to file
	certOut, err := os.Create(certFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatal(err)
	}
	certOut.Close()

	// Write key to file
	keyOut, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	privKeyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privKeyBytes}); err != nil {
		t.Fatal(err)
	}
	keyOut.Close()

	// Copy cert to CA file (self-signed)
	if err := os.WriteFile(caFile, []byte(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})), 0644); err != nil {
		t.Fatal(err)
	}

	// Load the generated certificate
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to load generated cert and key: %v", err)
	}

	// Create CA pool
	caCertPool := x509.NewCertPool()
	caCertPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	if !caCertPool.AppendCertsFromPEM(caCertPEM) {
		t.Fatal("Failed to append CA cert to pool")
	}

	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		MinVersion:   tls.VersionTLS12,
	}

	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.TLSConfig = tc
	opts.TLSHandshakeFirst = true // Enable TLS handshake first mode

	s := natsserver.RunServer(&opts)
	return s, certFile, keyFile, caFile
}
