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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"
)

// tlsCertValidity is how long generated certs are good for, measured from the
// moment they are minted. Long enough for a preserved instance or a long-running
// CI service to outlive a day, short enough to be unmistakably test-only.
const tlsCertValidity = 7 * 24 * time.Hour

// tlsMaterial bundles PEM-encoded cert material produced for one managed
// instance. ClientCertPEM and ClientKeyPEM are nil when mutual TLS was not
// requested.
type tlsMaterial struct {
	CAPEM         []byte
	ServerCertPEM []byte
	ServerKeyPEM  []byte
	ClientCertPEM []byte
	ClientKeyPEM  []byte
}

// defaultTLSSANs is the SAN set applied to a generated server cert when the
// caller supplies none. Treat it as immutable — copy before appending.
var defaultTLSSANs = []string{"localhost", "127.0.0.1", "::1"}

// generateTLSMaterial generates a self-signed CA, a server leaf signed by the
// CA with the supplied SANs, and — when mutual is true — a client leaf with
// ClientAuth EKU. SAN strings that parse as net.IP land in IPAddresses (v4
// addresses are normalised to 4-byte form); everything else goes to DNSNames.
// When sans is empty the defaults are ["localhost","127.0.0.1","::1"]. The
// CA signing key stays on the stack and is discarded on return — rotation is
// out of scope for the test harness.
func generateTLSMaterial(sans []string, mutual bool) (*tlsMaterial, error) {
	if len(sans) == 0 {
		sans = defaultTLSSANs
	}
	dnsSANs, ipSANs := classifySANs(sans)

	caCert, caKey, caPEM, err := generateCA()
	if err != nil {
		return nil, fmt.Errorf("generate CA: %w", err)
	}

	serverCertPEM, serverKeyPEM, err := signLeaf(caCert, caKey, leafSpec{
		commonName:  "ntf-server test server",
		dnsNames:    dnsSANs,
		ipAddresses: ipSANs,
		extKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		return nil, fmt.Errorf("sign server leaf: %w", err)
	}

	out := &tlsMaterial{
		CAPEM:         caPEM,
		ServerCertPEM: serverCertPEM,
		ServerKeyPEM:  serverKeyPEM,
	}

	if mutual {
		clientCertPEM, clientKeyPEM, err := signLeaf(caCert, caKey, leafSpec{
			commonName:  "ntf-server test client",
			extKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		})
		if err != nil {
			return nil, fmt.Errorf("sign client leaf: %w", err)
		}
		out.ClientCertPEM = clientCertPEM
		out.ClientKeyPEM = clientKeyPEM
	}

	return out, nil
}

func classifySANs(sans []string) ([]string, []net.IP) {
	var dns []string
	var ips []net.IP
	for _, s := range sans {
		if ip := net.ParseIP(s); ip != nil {
			if v4 := ip.To4(); v4 != nil {
				ips = append(ips, v4)
			} else {
				ips = append(ips, ip)
			}
			continue
		}
		dns = append(dns, s)
	}
	return dns, ips
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

func generateCA() (*x509.Certificate, *rsa.PrivateKey, []byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, err
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "ntf-server test CA"},
		NotBefore:             now.Add(-1 * time.Minute),
		NotAfter:              now.Add(tlsCertValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, err
	}
	return cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

type leafSpec struct {
	commonName  string
	dnsNames    []string
	ipAddresses []net.IP
	extKeyUsage []x509.ExtKeyUsage
}

func signLeaf(caCert *x509.Certificate, caKey *rsa.PrivateKey, spec leafSpec) ([]byte, []byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: spec.commonName},
		NotBefore:             now.Add(-1 * time.Minute),
		NotAfter:              now.Add(tlsCertValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           spec.extKeyUsage,
		BasicConstraintsValid: true,
		DNSNames:              spec.dnsNames,
		IPAddresses:           spec.ipAddresses,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}
