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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// customSystemAccounts replaces the built-in template's accounts with an APP
// account, the default user's, and a $SYS account whose user is not the
// built-in template's.
var customSystemAccounts = map[string]string{
	"accounts": `accounts {
	APP { users = [ { user: "app", pass: "app" } ], jetstream: enabled }
	$SYS { users = [ { user: "admin", pass: "s3cret" } ] }
}`,
	"authorization": "no_auth_user: app",
}

// jetStreamRequestAtOnce makes one JetStream request on a fresh connection to
// url, with opts, and no retry: it reads the account info and creates an R3
// stream, the requests that fail with 10008 until a meta leader is elected
// and its peers are current.
func jetStreamRequestAtOnce(t *testing.T, url string, replicas int, opts ...nats.Option) {
	t.Helper()

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err = js.AccountInfo(ctx)
	if err != nil {
		t.Fatalf("account info at once: %v", err)
	}

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "WAITED", Subjects: []string{"waited"}, Replicas: replicas})
	if err != nil {
		t.Fatalf("create R%d stream at once: %v", replicas, err)
	}
}

// createError sends a create expected to fail and returns its code, its
// description and its data.
func createError(t *testing.T, nc *nats.Conn, subject string, req any) (string, string, []byte) {
	t.Helper()

	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg, err := nc.Request(subject, payload, 60*time.Second)
	if err != nil {
		t.Fatalf("request %s: %v", subject, err)
	}

	return msg.Header.Get("Nats-Service-Error-Code"), msg.Header.Get("Nats-Service-Error"), msg.Data
}

// instanceIDs lists the ids of the service's instances.
func instanceIDs(t *testing.T, svc *Service) []string {
	t.Helper()

	var list api.ListResponse
	mustRequest(t, svc.nc, "tester.list", api.ListRequest{}, &list)

	var ids []string
	for _, inst := range list.Instances {
		ids = append(ids, inst.ID)
	}

	return ids
}

// TestCreateClusterWaitsForJetStream proves a three-node JetStream cluster
// created with wait_jetstream answers once JetStream takes requests: an
// account info and an R3 stream create on a fresh connection succeed at once,
// with no retry.
func TestCreateClusterWaitsForJetStream(t *testing.T) {
	svc := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 3, JetStream: true, WaitJetStream: 30}, &created)

	jetStreamRequestAtOnce(t, fmt.Sprintf("nats://localhost:%d", created.Servers[0].Port), 3)
}

// TestCreateSuperClusterWaitsForJetStream proves a super-cluster created with
// wait_jetstream answers once JetStream takes requests across its gateways.
func TestCreateSuperClusterWaitsForJetStream(t *testing.T) {
	svc := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.super-cluster", api.CreateSuperClusterRequest{Clusters: 2, Servers: 2, JetStream: true, WaitJetStream: 60}, &created)

	jetStreamRequestAtOnce(t, fmt.Sprintf("nats://localhost:%d", created.Servers[len(created.Servers)-1].Port), 2)
}

// TestCreateServerWaitsForJetStream proves a lone server, which runs JetStream
// without a meta group, is ready once it reports JetStream enabled.
func TestCreateServerWaitsForJetStream(t *testing.T) {
	svc := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.server", api.CreateServerRequest{JetStream: true, WaitJetStream: 10}, &created)

	jetStreamRequestAtOnce(t, fmt.Sprintf("nats://localhost:%d", created.Servers[0].Port), 1)
}

// TestCreateWaitsForJetStreamOverGeneratedTLS proves the wait dials the nodes
// of an instance with generated mutual TLS, handshake first, with the
// instance's own material.
func TestCreateWaitsForJetStreamOverGeneratedTLS(t *testing.T) {
	svc := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{
		Servers:       3,
		JetStream:     true,
		TLS:           &api.TLSOptions{Mode: api.TLSModeMutual, HandshakeFirst: true},
		WaitJetStream: 30,
	}, &created)

	cfg, err := waitTLSConfig(created.TLS, defaultTLSSANs, "localhost")
	if err != nil {
		t.Fatalf("tls config: %v", err)
	}
	jetStreamRequestAtOnce(t, fmt.Sprintf("tls://localhost:%d", created.Servers[0].Port), 3, nats.Secure(cfg), nats.TLSHandshakeFirst())
}

// TestCreateWaitsForJetStreamWithSystemCredentials proves a create whose
// accounts snippet replaces the system user waits with the system
// credentials it gives, and that wrong ones, which every server refuses, end
// a long wait at once naming the refusal.
func TestCreateWaitsForJetStreamWithSystemCredentials(t *testing.T) {
	svc := startTestService(t)

	var created api.CreateResponse
	mustRequest(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{
		Servers:           3,
		JetStream:         true,
		Snippets:          customSystemAccounts,
		WaitJetStream:     30,
		SystemCredentials: &api.Credential{UserPassword: &api.UserPassword{User: "admin", Password: "s3cret"}},
	}, &created)

	jetStreamRequestAtOnce(t, fmt.Sprintf("nats://localhost:%d", created.Servers[0].Port), 3)

	start := time.Now()
	code, desc, _ := createError(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{
		Servers:           3,
		JetStream:         true,
		Snippets:          customSystemAccounts,
		WaitJetStream:     30,
		SystemCredentials: &api.Credential{UserPassword: &api.UserPassword{User: "admin", Password: "wrong"}},
	})
	took := time.Since(start)
	if code != "024" {
		t.Fatalf("wrong credentials answered %q %q, want 024", code, desc)
	}
	if took > 15*time.Second {
		t.Errorf("wrong credentials answered after %v, want at once rather than at the 30s timeout", took)
	}
	if !strings.Contains(desc, "every server refused the system credentials") || !strings.Contains(strings.ToLower(desc), "authorization violation") {
		t.Errorf("wrong credentials answered %q, want the refusal", desc)
	}
}

// waitNeverSatisfied is the system credentials of a user outside the system
// account: every server accepts them, and none answers a JSZ ping sent in the
// user's account, so a wait with them runs until it is ended.
var waitNeverSatisfied = &api.Credential{UserPassword: &api.UserPassword{User: "user1", Password: "password"}}

// TestCreateWaitForJetStreamTimesOut proves a wait too short for a meta leader
// to be elected fails the create with 024, naming every node not ready, with
// the create response as its data, and leaves the instance in place.
func TestCreateWaitForJetStreamTimesOut(t *testing.T) {
	svc := startTestService(t)

	code, desc, data := createError(t, svc.nc, "tester.create.cluster", api.CreateClusterRequest{Servers: 3, JetStream: true, WaitJetStream: 0.001})
	if code != "024" {
		t.Fatalf("error code = %q (%s), want 024", code, desc)
	}

	var created api.CreateResponse
	err := json.Unmarshal(data, &created)
	if err != nil {
		t.Fatalf("024 data is not a create response: %v: %q", err, data)
	}
	if created.ID == "" || len(created.Servers) != 3 {
		t.Fatalf("024 data is %+v, want the instance with 3 servers", created)
	}

	if !strings.Contains(desc, created.ID) || !strings.Contains(desc, errJetStreamNotReady.Error()) {
		t.Errorf("description %q does not name the instance and %q", desc, errJetStreamNotReady)
	}
	for _, s := range created.Servers {
		if !strings.Contains(desc, s.Name+": ") {
			t.Errorf("description %q does not name node %s", desc, s.Name)
		}
	}

	if !slices.Contains(instanceIDs(t, svc), created.ID) {
		t.Fatalf("instance %s is gone after the wait timed out", created.ID)
	}

	var destroyed api.DestroyResponse
	mustRequest(t, svc.nc, "tester.destroy", api.DestroyRequest{InstanceID: created.ID}, &destroyed)
	if !destroyed.Destroyed {
		t.Error("the instance left by the wait could not be destroyed")
	}
}

// TestCreateWaitForJetStreamEndsOnDestroy proves a destroy of an instance
// while its create waits for JetStream ends the wait with 404, long before
// the wait would have timed out. System credentials outside the system
// account keep the wait from ever being satisfied.
func TestCreateWaitForJetStreamEndsOnDestroy(t *testing.T) {
	svc := startTestService(t)

	type answer struct {
		code string
		desc string
	}
	done := make(chan answer, 1)
	go func() {
		payload, _ := json.Marshal(api.CreateClusterRequest{
			Servers:           2,
			JetStream:         true,
			WaitJetStream:     120,
			SystemCredentials: waitNeverSatisfied,
		})
		msg, err := svc.nc.Request("tester.create.cluster", payload, 60*time.Second)
		if err != nil {
			done <- answer{desc: err.Error()}
			return
		}
		done <- answer{msg.Header.Get("Nats-Service-Error-Code"), msg.Header.Get("Nats-Service-Error")}
	}()

	var id string
	deadline := time.Now().Add(30 * time.Second)
	for id == "" {
		ids := instanceIDs(t, svc)
		if len(ids) > 0 {
			id = ids[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the waiting create's instance was never listed")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The instance is listed before its servers are published; a destroy
	// before then would answer the create with 404 as well, without a wait.
	// Waiting for its servers proves the wait itself ends.
	deadline = time.Now().Add(30 * time.Second)
	for {
		var status api.StatusResponse
		mustRequest(t, svc.nc, "tester.status", api.StatusRequest{InstanceID: id}, &status)
		if len(status.Instances) == 1 && len(status.Instances[0].Servers) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the waiting create's servers were never published")
		}
		time.Sleep(50 * time.Millisecond)
	}

	var destroyed api.DestroyResponse
	mustRequest(t, svc.nc, "tester.destroy", api.DestroyRequest{InstanceID: id}, &destroyed)

	select {
	case got := <-done:
		if got.code != "404" {
			t.Errorf("the waiting create answered %q %q, want 404", got.code, got.desc)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the waiting create did not answer after its instance was destroyed")
	}
}

// TestCloseEndsJetStreamWait proves Close ends a create's JetStream wait
// rather than holding until it times out.
func TestCloseEndsJetStreamWait(t *testing.T) {
	svc, err := New(t.Context(), Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Close runs below; this one releases the service when the test fails
	// before it, and does nothing after it.
	t.Cleanup(func() { svc.Close() })

	nc, err := nats.Connect(svc.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	payload, _ := json.Marshal(api.CreateClusterRequest{
		Servers:           2,
		JetStream:         true,
		WaitJetStream:     120,
		SystemCredentials: waitNeverSatisfied,
	})
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	err = nc.PublishRequest("tester.create.cluster", inbox, payload)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wait for the servers to be published, so the create is waiting.
	deadline := time.Now().Add(30 * time.Second)
	for {
		var status api.StatusResponse
		mustRequest(t, nc, "tester.status", api.StatusRequest{}, &status)
		if len(status.Instances) == 1 && len(status.Instances[0].Servers) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the waiting create's servers were never published")
		}
		time.Sleep(50 * time.Millisecond)
	}

	start := time.Now()
	err = svc.Close()
	if err != nil {
		t.Errorf("Close: %v", err)
	}
	if took := time.Since(start); took > 20*time.Second {
		t.Errorf("Close took %v with a create waiting for JetStream", took)
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("the waiting create did not answer: %v", err)
	}
	code, desc := msg.Header.Get("Nats-Service-Error-Code"), msg.Header.Get("Nats-Service-Error")
	if code != "024" || !strings.Contains(desc, "when the service closed") {
		t.Errorf("the waiting create answered %q %q, want 024 naming the close", code, desc)
	}
}

// TestJetStreamWaitRefusals proves every kind of create refuses a wait it
// cannot carry out before it starts anything: without JetStream with 011,
// with a system account it has no credentials for, with a tls snippet, and
// credentials without a wait with 002, and a wait or credentials its schema
// refuses with 001.
func TestJetStreamWaitRefusals(t *testing.T) {
	svc := startTestService(t)

	creds := &api.Credential{UserPassword: &api.UserPassword{User: "admin", Password: "s3cret"}}

	type fields struct {
		jetStream bool
		wait      float64
		creds     *api.Credential
		snippets  map[string]string
		template  string
	}
	tests := []struct {
		name string
		f    fields
		code string
		desc string
	}{
		{"without jetstream", fields{wait: 5}, "011", "Invalid request: wait_jetstream needs jetstream"},
		{"accounts snippet", fields{jetStream: true, wait: 5, snippets: map[string]string{"accounts": "accounts {}"}}, "002", "Invalid request: wait_jetstream with the accounts snippet needs system_credentials"},
		{"system_account snippet", fields{jetStream: true, wait: 5, snippets: map[string]string{"system_account": `system_account: "SYS"`}}, "002", "Invalid request: wait_jetstream with the system_account snippet needs system_credentials"},
		{"custom template", fields{jetStream: true, wait: 5, template: serverConfigTemplate}, "002", "Invalid request: wait_jetstream with a custom template needs system_credentials"},
		{"tls snippet", fields{jetStream: true, wait: 5, creds: creds, snippets: map[string]string{"tls": "tls {}"}}, "002", "Invalid request: wait_jetstream cannot dial a server whose TLS comes from a tls snippet: use generated tls instead"},
		{"credentials without a wait", fields{jetStream: true, creds: creds}, "002", "Invalid request: system_credentials needs wait_jetstream"},
		{"wait over the maximum", fields{jetStream: true, wait: 301}, "001", "Invalid request: wait_jetstream: maximum: got 301, want 300"},
		{"negative wait", fields{jetStream: true, wait: -1}, "001", ""},
		{"credentials without a type", fields{jetStream: true, wait: 5, creds: &api.Credential{}}, "001", ""},
		{"credentials without a user", fields{jetStream: true, wait: 5, creds: &api.Credential{UserPassword: &api.UserPassword{Password: "p"}}}, "001", ""},
	}

	requests := map[string]func(f fields) any{
		"tester.create.server": func(f fields) any {
			return api.CreateServerRequest{JetStream: f.jetStream, WaitJetStream: f.wait, SystemCredentials: f.creds, Snippets: f.snippets, Template: f.template}
		},
		"tester.create.cluster": func(f fields) any {
			return api.CreateClusterRequest{Servers: 2, JetStream: f.jetStream, WaitJetStream: f.wait, SystemCredentials: f.creds, Snippets: f.snippets, Template: f.template}
		},
		"tester.create.super-cluster": func(f fields) any {
			return api.CreateSuperClusterRequest{Clusters: 2, Servers: 2, JetStream: f.jetStream, WaitJetStream: f.wait, SystemCredentials: f.creds, Snippets: f.snippets, Template: f.template}
		},
	}

	for _, subject := range slices.Sorted(maps.Keys(requests)) {
		for _, tt := range tests {
			t.Run(subject+"/"+tt.name, func(t *testing.T) {
				code, desc := serviceError(t, svc.nc, subject, requests[subject](tt.f))
				if code != tt.code {
					t.Errorf("error code = %q (%s), want %q", code, desc, tt.code)
				}
				if tt.desc != "" && desc != tt.desc {
					t.Errorf("error description = %q, want %q", desc, tt.desc)
				}
			})
		}
	}

	if ids := instanceIDs(t, svc); len(ids) != 0 {
		t.Errorf("refused creates left instances %v", ids)
	}
}

// TestNewJetStreamWait proves the wait a create asks for: none without a
// wait, the built-in system user without credentials, and the credentials
// given otherwise, with the timeout in seconds, fractions honored.
func TestNewJetStreamWait(t *testing.T) {
	w, refusal := newJetStreamWait(0, nil, true, nil, "")
	if w != nil || refusal != nil {
		t.Errorf("no wait gave %+v, %v", w, refusal)
	}

	w, refusal = newJetStreamWait(1.5, nil, true, map[string]string{"authorization": "no_auth_user: user2"}, "")
	if refusal != nil {
		t.Fatalf("refused: %v", refusal)
	}
	want := jetStreamWait{timeout: 1500 * time.Millisecond, user: "system", password: "password"}
	if *w != want {
		t.Errorf("wait %+v, want %+v", *w, want)
	}

	w, refusal = newJetStreamWait(2, &api.Credential{UserPassword: &api.UserPassword{User: "admin", Password: "s3cret"}}, true, customSystemAccounts, serverConfigTemplate)
	if refusal != nil {
		t.Fatalf("refused: %v", refusal)
	}
	want = jetStreamWait{timeout: 2 * time.Second, user: "admin", password: "s3cret"}
	if *w != want {
		t.Errorf("wait %+v, want %+v", *w, want)
	}
}

// jsz returns the JSZ reply of the server named name with JetStream enabled,
// naming leader as its meta leader and holding replicas.
func jsz(name, leader string, replicas ...*server.PeerInfo) *server.ServerAPIJszResponse {
	return &server.ServerAPIJszResponse{
		Server: &server.ServerInfo{Name: name},
		Data:   &server.JSInfo{Meta: &server.MetaClusterInfo{Leader: leader, Replicas: replicas}},
	}
}

// TestJetStreamNotReady proves which nodes the wait finds not ready, and why,
// from the JSZ replies of a round.
func TestJetStreamNotReady(t *testing.T) {
	nodes := []waitNode{{name: "n1", serverName: "n1"}, {name: "n2", serverName: "n2"}, {name: "n3", serverName: "s3"}}
	current := func(name string) *server.PeerInfo { return &server.PeerInfo{Name: name, Current: true} }

	tests := []struct {
		name    string
		nodes   []waitNode
		replies map[string]*server.ServerAPIJszResponse
		want    map[string]string
	}{
		{
			"ready",
			nodes,
			map[string]*server.ServerAPIJszResponse{"n1": jsz("n1", "n1", current("n2"), current("s3")), "n2": jsz("n2", "n1"), "s3": jsz("s3", "n1")},
			map[string]string{},
		},
		{
			"no replies",
			nodes,
			nil,
			map[string]string{"n1": "no reply", "n2": "no reply", "n3": "no reply"},
		},
		{
			"no leader yet",
			nodes,
			map[string]*server.ServerAPIJszResponse{"n1": jsz("n1", ""), "n2": jsz("n2", ""), "s3": jsz("s3", "")},
			map[string]string{"n1": "no meta leader", "n2": "no meta leader", "n3": "no meta leader"},
		},
		{
			"peer not current",
			nodes,
			map[string]*server.ServerAPIJszResponse{"n1": jsz("n1", "n1", current("n2"), &server.PeerInfo{Name: "s3"}), "n2": jsz("n2", "n1"), "s3": jsz("s3", "n1")},
			map[string]string{"n3": "not current on meta leader n1"},
		},
		{
			"peer offline",
			nodes,
			map[string]*server.ServerAPIJszResponse{"n1": jsz("n1", "n1", current("n2"), &server.PeerInfo{Name: "s3", Current: true, Offline: true}), "n2": jsz("n2", "n1"), "s3": jsz("s3", "n1")},
			map[string]string{"n3": "offline on meta leader n1"},
		},
		{
			"peer unknown to the leader",
			nodes,
			map[string]*server.ServerAPIJszResponse{"n1": jsz("n1", "n1", current("n2")), "n2": jsz("n2", "n1"), "s3": jsz("s3", "n1")},
			map[string]string{"n3": "not a peer of meta leader n1"},
		},
		{
			"leader silent",
			nodes,
			map[string]*server.ServerAPIJszResponse{"n2": jsz("n2", "n1"), "s3": jsz("s3", "n1")},
			map[string]string{"n1": "no reply", "n2": "meta leader n1 not confirmed", "n3": "meta leader n1 not confirmed"},
		},
		{
			"leaders disagree",
			nodes,
			map[string]*server.ServerAPIJszResponse{"n1": jsz("n1", "n1", current("n2"), current("s3")), "n2": jsz("n2", "n1"), "s3": jsz("s3", "n2")},
			map[string]string{"n3": "sees meta leader n2, not n1"},
		},
		{
			"two claim the lead",
			nodes,
			map[string]*server.ServerAPIJszResponse{"n1": jsz("n1", "n1"), "n2": jsz("n2", "n2"), "s3": jsz("s3", "n1")},
			map[string]string{"n1": "meta leader n1 not confirmed", "n2": "meta leader n2 not confirmed", "n3": "meta leader n1 not confirmed"},
		},
		{
			"jetstream disabled",
			nodes[:1],
			map[string]*server.ServerAPIJszResponse{"n1": {Server: &server.ServerInfo{Name: "n1"}, Data: &server.JSInfo{Disabled: true}}},
			map[string]string{"n1": "JetStream not enabled"},
		},
		{
			"error reply",
			nodes[:1],
			map[string]*server.ServerAPIJszResponse{"n1": {Server: &server.ServerInfo{Name: "n1"}, Error: &server.ApiError{Code: 500, Description: "boom"}}},
			map[string]string{"n1": "replied with error: boom"},
		},
		{
			"lone server without a meta group",
			nodes[:1],
			map[string]*server.ServerAPIJszResponse{"n1": {Server: &server.ServerInfo{Name: "n1"}, Data: &server.JSInfo{}}},
			map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := jetStreamNotReady(tt.nodes, tt.replies)
			if !maps.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// TestNotReadyError proves the error a wait gives up with wraps
// errJetStreamNotReady and names each node not ready in node order, with the
// dial failure when it held no connection.
func TestNotReadyError(t *testing.T) {
	nodes := []waitNode{{name: "n1"}, {name: "n2"}, {name: "n3"}}

	err := notReadyError("after 1s", nodes, map[string]string{"n3": "no reply", "n1": "no meta leader"}, false, errors.New("ignored"))
	if !errors.Is(err, errJetStreamNotReady) {
		t.Errorf("error %v does not wrap errJetStreamNotReady", err)
	}
	want := "JetStream not ready after 1s: n1: no meta leader; n3: no reply"
	if err.Error() != want {
		t.Errorf("error %q, want %q", err, want)
	}

	err = notReadyError("after 1s", nodes[:1], map[string]string{"n1": "no reply"}, true, errors.New("n1: refused"))
	want = "JetStream not ready after 1s: n1: no reply (could not connect: n1: refused)"
	if err.Error() != want {
		t.Errorf("error %q, want %q", err, want)
	}
}

// TestMoreInformative proves a dial whose timeout the wait's deadline cut
// short never hides an earlier failure that is not a timeout, and that any
// other failure is kept as the latest.
func TestMoreInformative(t *testing.T) {
	refused := fmt.Errorf("n1: %w", nats.ErrAuthorization)
	timedOut := fmt.Errorf("n2: %w", nats.ErrTimeout)
	other := errors.New("n3: connection refused")

	tests := []struct {
		name       string
		prev, next error
		want       error
	}{
		{"first error", nil, refused, refused},
		{"no new error", refused, nil, refused},
		{"timeout after a refusal", refused, timedOut, refused},
		{"refusal after a timeout", timedOut, refused, refused},
		{"timeout after a timeout", timedOut, timedOut, timedOut},
		{"other after a refusal", refused, other, other},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := moreInformative(tt.prev, tt.next)
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}

	if !isAuthError(refused) || isAuthError(timedOut) {
		t.Error("isAuthError does not tell a refusal from a timeout")
	}
}
