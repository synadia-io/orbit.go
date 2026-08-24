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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/synadia-io/orbit.go/ntf/api"
)

func TestEffectiveSANs(t *testing.T) {
	t.Run("defaults when none requested", func(t *testing.T) {
		got := effectiveSANs(nil, "")
		if !sliceEqual(got, []string{"localhost", "127.0.0.1", "::1"}) {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("appends advertise host to defaults", func(t *testing.T) {
		got := effectiveSANs(nil, "ci-host.example")
		if !sliceEqual(got, []string{"localhost", "127.0.0.1", "::1", "ci-host.example"}) {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("does not duplicate an advertise host already present", func(t *testing.T) {
		got := effectiveSANs(nil, "localhost")
		if !sliceEqual(got, []string{"localhost", "127.0.0.1", "::1"}) {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("appends advertise host to caller SANs", func(t *testing.T) {
		got := effectiveSANs([]string{"node-a.test"}, "ci-host.example")
		if !sliceEqual(got, []string{"node-a.test", "ci-host.example"}) {
			t.Fatalf("got %v", got)
		}
	})

	t.Run("does not mutate the shared default SAN slice", func(t *testing.T) {
		before := append([]string(nil), defaultTLSSANs...)
		_ = effectiveSANs(nil, "mutator.example")
		if !sliceEqual(defaultTLSSANs, before) {
			t.Fatalf("defaultTLSSANs mutated: got %v want %v", defaultTLSSANs, before)
		}
	})
}

// TestSetupInstanceTLSAddsAdvertiseHostToSANs proves the resolved advertise
// host lands in the minted server cert SANs (so cross-server TLS discovery to
// the advertised address verifies) while the built-in defaults are retained.
func TestSetupInstanceTLSAddsAdvertiseHostToSANs(t *testing.T) {
	s := &Service{log: slog.New(slog.DiscardHandler)}
	inst := &instance{ID: "tls-advertise-test", RootDir: t.TempDir()}

	files, _, err := s.setupInstanceTLS(inst, &api.TLSOptions{Mode: api.TLSModeServer}, "ci-host.example")
	if err != nil {
		t.Fatalf("setupInstanceTLS: %v", err)
	}

	pemBytes, err := os.ReadFile(files.serverCert)
	if err != nil {
		t.Fatalf("read server cert: %v", err)
	}
	cert := parseFirstCert(t, pemBytes)

	if !slices.Contains(cert.DNSNames, "ci-host.example") {
		t.Fatalf("advertise host missing from cert DNS SANs: %v", cert.DNSNames)
	}
	if !slices.Contains(cert.DNSNames, "localhost") {
		t.Fatalf("default localhost SAN missing from cert: %v", cert.DNSNames)
	}
}

func TestGenerateTLS_DefaultSANs(t *testing.T) {
	m, err := generateTLSMaterial(nil, true)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	cert := parseFirstCert(t, m.ServerCertPEM)

	wantDNS := []string{"localhost"}
	if !sliceEqual(cert.DNSNames, wantDNS) {
		t.Fatalf("DNS SANs: got %v want %v", cert.DNSNames, wantDNS)
	}
	wantIPs := []string{"127.0.0.1", "::1"}
	gotIPs := make([]string, 0, len(cert.IPAddresses))
	for _, ip := range cert.IPAddresses {
		gotIPs = append(gotIPs, ip.String())
	}
	if !sliceEqual(gotIPs, wantIPs) {
		t.Fatalf("IP SANs: got %v want %v", gotIPs, wantIPs)
	}
}

func TestGenerateTLS_MutualIncludesClient(t *testing.T) {
	m, err := generateTLSMaterial(nil, true)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if m.ClientCertPEM == nil || m.ClientKeyPEM == nil {
		t.Fatalf("expected client material for mutual=true")
	}
	if _, err := tls.X509KeyPair(m.ClientCertPEM, m.ClientKeyPEM); err != nil {
		t.Fatalf("client X509KeyPair: %v", err)
	}
}

func TestGenerateTLS_NonMutualSkipsClient(t *testing.T) {
	m, err := generateTLSMaterial(nil, false)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if m.ClientCertPEM != nil || m.ClientKeyPEM != nil {
		t.Fatalf("expected no client material for mutual=false")
	}
}

func TestGenerateTLS_CustomSANs(t *testing.T) {
	sans := []string{"node-a.test", "10.0.0.5", "::1"}
	m, err := generateTLSMaterial(sans, false)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	cert := parseFirstCert(t, m.ServerCertPEM)

	if !sliceEqual(cert.DNSNames, []string{"node-a.test"}) {
		t.Fatalf("DNS SANs: got %v", cert.DNSNames)
	}
	ips := make([]string, 0, len(cert.IPAddresses))
	for _, ip := range cert.IPAddresses {
		ips = append(ips, ip.String())
	}
	if !sliceEqual(ips, []string{"10.0.0.5", "::1"}) {
		t.Fatalf("IP SANs: got %v", ips)
	}
}

func TestGenerateTLS_ServerCertChainVerifies(t *testing.T) {
	m, err := generateTLSMaterial([]string{"localhost"}, false)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(m.CAPEM) {
		t.Fatalf("could not load CA into pool")
	}
	server := parseFirstCert(t, m.ServerCertPEM)
	if _, err := server.Verify(x509.VerifyOptions{
		Roots:       pool,
		DNSName:     "localhost",
		CurrentTime: time.Now(),
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Fatalf("server verify: %v", err)
	}
}

func TestGenerateTLS_ClientCertChainVerifies(t *testing.T) {
	m, err := generateTLSMaterial(nil, true)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(m.CAPEM) {
		t.Fatalf("could not load CA into pool")
	}
	client := parseFirstCert(t, m.ClientCertPEM)
	if _, err := client.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		t.Fatalf("client verify: %v", err)
	}
}

func TestGenerateTLS_ServerCertHasServerAuth(t *testing.T) {
	m, err := generateTLSMaterial(nil, false)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	cert := parseFirstCert(t, m.ServerCertPEM)
	if !hasEKU(cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
		t.Fatalf("server cert missing ServerAuth EKU: %v", cert.ExtKeyUsage)
	}
}

func TestGenerateTLS_ClientCertHasClientAuth(t *testing.T) {
	m, err := generateTLSMaterial(nil, true)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	cert := parseFirstCert(t, m.ClientCertPEM)
	if !hasEKU(cert.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		t.Fatalf("client cert missing ClientAuth EKU: %v", cert.ExtKeyUsage)
	}
}

func TestGenerateTLS_CAIsCA(t *testing.T) {
	m, err := generateTLSMaterial(nil, false)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	ca := parseFirstCert(t, m.CAPEM)
	if !ca.IsCA {
		t.Fatalf("CA cert not marked IsCA")
	}
	if !ca.BasicConstraintsValid {
		t.Fatalf("CA cert BasicConstraintsValid=false")
	}
	if ca.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Fatalf("CA cert missing CertSign KeyUsage")
	}
}

func TestGenerateTLS_ServerKeyPairLoads(t *testing.T) {
	m, err := generateTLSMaterial(nil, false)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if _, err := tls.X509KeyPair(m.ServerCertPEM, m.ServerKeyPEM); err != nil {
		t.Fatalf("server X509KeyPair: %v", err)
	}
}

func parseFirstCert(t *testing.T, pemBytes []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		t.Fatalf("could not decode PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	return cert
}

func hasEKU(ekus []x509.ExtKeyUsage, want x509.ExtKeyUsage) bool {
	for _, e := range ekus {
		if e == want {
			return true
		}
	}
	return false
}

func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
