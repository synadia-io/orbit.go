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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/synadia-io/orbit.go/ntf/api"
	"github.com/synadia-io/orbit.go/ntf/backend"
	"github.com/synadia-io/orbit.go/ntf/schema/validate"
)

const (
	// maxJetStreamWait is the longest wait_jetstream a create may ask for,
	// the maximum its schema allows. A create endpoint answers one request at
	// a time until a create's servers are published, and the wait runs after
	// that, but the caller still holds its request open for the whole wait.
	maxJetStreamWait = 5 * time.Minute

	// jszPingSubject asks every server reachable from the system account for
	// its JetStream state, one reply per server.
	jszPingSubject = "$SYS.REQ.SERVER.PING.JSZ"

	// jszGatherWindow bounds how long one round of the wait gathers JSZ
	// replies. A round ends sooner once every node replied.
	jszGatherWindow = time.Second

	// jetStreamWaitInterval is the pause between two rounds that found
	// JetStream not ready.
	jetStreamWaitInterval = 250 * time.Millisecond

	// jetStreamWaitDialTimeout bounds one dial of a node by the wait.
	jetStreamWaitDialTimeout = 2 * time.Second

	// jetStreamWaitConnName names the connections the wait opens.
	jetStreamWaitConnName = "ntf-jetstream-wait"

	// defaultSystemUser and defaultSystemPassword are the system user of the
	// built-in template's $SYS account, which the wait authenticates as when
	// a create gives no system credentials.
	defaultSystemUser     = "system"
	defaultSystemPassword = "password"
)

// systemAccountSnippets are the snippet keys that replace the built-in
// template's system account or its system user, so a create using one needs
// system credentials to wait for JetStream.
var systemAccountSnippets = []string{"accounts", "system_account"}

// jetStreamWait is how a create with wait_jetstream waits for JetStream once
// its servers are ready.
type jetStreamWait struct {
	// timeout is how long the wait lasts at most.
	timeout time.Duration

	// user and password authenticate the wait to the system account.
	user     string
	password string
}

// newJetStreamWait returns the wait a create asks for with seconds and creds,
// nil when seconds is zero, or why the create is refused: 011 when the
// instance has no JetStream, and 002 when the wait cannot authenticate or
// cannot dial the nodes. A custom template or an accounts or system_account
// snippet replaces the built-in template's system user, so without creds it
// is refused rather than waited on until it times out. A tls snippet is
// refused because the service has neither the CA nor a client certificate to
// dial it with; generated TLS is dialed with its own material.
func newJetStreamWait(seconds float64, creds *api.Credential, jetStream bool, snippets map[string]string, customTemplate string) (*jetStreamWait, *specError) {
	if seconds == 0 {
		if creds != nil {
			return nil, &specError{Code: "002", Message: "system_credentials needs wait_jetstream"}
		}
		return nil, nil
	}

	timeout := time.Duration(seconds * float64(time.Second))
	if seconds < 0 || timeout > maxJetStreamWait {
		return nil, &specError{Code: "002", Message: fmt.Sprintf("wait_jetstream should be from 0 to %v seconds", maxJetStreamWait.Seconds())}
	}

	if !jetStream {
		return nil, &specError{Code: "011", Message: "wait_jetstream needs jetstream"}
	}

	if body, ok := snippets["tls"]; ok && body != "" {
		return nil, &specError{Code: "002", Message: "wait_jetstream cannot dial a server whose TLS comes from a tls snippet: use generated tls instead"}
	}

	w := &jetStreamWait{timeout: timeout, user: defaultSystemUser, password: defaultSystemPassword}

	if creds != nil {
		if creds.UserPassword == nil || creds.UserPassword.User == "" {
			return nil, &specError{Code: "002", Message: "system_credentials needs a user_password with a user"}
		}
		w.user = creds.UserPassword.User
		w.password = creds.UserPassword.Password
		return w, nil
	}

	if customTemplate != "" {
		return nil, &specError{Code: "002", Message: "wait_jetstream with a custom template needs system_credentials"}
	}
	for _, key := range systemAccountSnippets {
		if _, ok := snippets[key]; ok {
			return nil, &specError{Code: "002", Message: fmt.Sprintf("wait_jetstream with the %s snippet needs system_credentials", key)}
		}
	}

	return w, nil
}

// waitNode is a node of an instance the JetStream wait expects to reply.
type waitNode struct {
	// name is the node's name, as the create response names it.
	name string

	// serverName is the name the server reports, which its JSZ reply carries.
	// A custom template may set it apart from name.
	serverName string

	// client is the node's own client listener, behind any proxy.
	client backend.Endpoint
}

// waitDial is how the JetStream wait dials a node: as the system user, and
// over TLS when the instance has generated TLS.
type waitDial struct {
	user     string
	password string

	// tls is the instance's generated TLS material, nil without generated
	// TLS, and sans the names its server certificate holds.
	tls            *api.TLSMaterial
	sans           []string
	handshakeFirst bool
}

// waitTLSConfig returns the config the wait dials an instance with generated
// TLS with: its CA, its client certificate under mutual TLS, and a server
// name the server certificate holds, host when it is among sans and the first
// of sans otherwise, since the wait dials the node's internal address.
func waitTLSConfig(mat *api.TLSMaterial, sans []string, host string) (*tls.Config, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(mat.CAPEM)) {
		return nil, fmt.Errorf("the generated CA does not parse")
	}

	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}

	if mat.ClientCertPEM != "" {
		cert, err := tls.X509KeyPair([]byte(mat.ClientCertPEM), []byte(mat.ClientKeyPEM))
		if err != nil {
			return nil, fmt.Errorf("the generated client certificate does not parse: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	switch {
	case slices.Contains(sans, host):
		cfg.ServerName = host
	case len(sans) > 0:
		cfg.ServerName = sans[0]
	}

	return cfg, nil
}

// connect dials node and returns the connection, which never reconnects. The
// TCP dial ends when ctx does.
func (d waitDial) connect(ctx context.Context, node waitNode, timeout time.Duration) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.UserInfo(d.user, d.password),
		nats.Name(jetStreamWaitConnName),
		nats.NoReconnect(),
		nats.IgnoreDiscoveredServers(),
		nats.Timeout(timeout),
		nats.SetCustomDialer(ctxDialer{ctx: ctx, timeout: timeout}),
	}
	if d.tls != nil {
		cfg, err := waitTLSConfig(d.tls, d.sans, node.client.Host)
		if err != nil {
			return nil, err
		}
		opts = append(opts, nats.Secure(cfg))
		if d.handshakeFirst {
			opts = append(opts, nats.TLSHandshakeFirst())
		}
	}

	return nats.Connect("nats://"+node.client.String(), opts...)
}

// ctxDialer dials TCP until ctx ends or timeout passes, so a dial of the wait
// ends when the service closes.
type ctxDialer struct {
	ctx     context.Context
	timeout time.Duration
}

// Dial dials address on network.
func (d ctxDialer) Dial(network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(d.ctx, d.timeout)
	defer cancel()

	var dialer net.Dialer
	return dialer.DialContext(ctx, network, address)
}

// answerAfterJetStreamWait waits with w for JetStream on the nodes of inst,
// then answers req with resp, the create response. A wait that times out, that
// every node refuses the system credentials of, or that the service's Close
// ends, answers 024 naming each node not ready and why, with resp as the
// error's data so the caller can still destroy or inspect the instance, which
// is left in place. A wait the instance's destroy ends answers 404.
func (s *Service) answerAfterJetStreamWait(req micro.Request, inst *instance, w *jetStreamWait, dial waitDial, nodes []waitNode, resp api.CreateResponse) {
	start := time.Now()
	err := s.waitJetStream(inst.ID, w.timeout, dial, nodes)
	switch {
	case err == nil:
		s.log.Info("JetStream ready", "instance", inst.ID, "duration", time.Since(start))
		req.RespondJSON(resp)
	case errors.Is(err, errInstanceDestroyed):
		req.Error("404", "Instance destroyed while waiting for JetStream", nil)
	default:
		s.log.Warn("JetStream not ready", "instance", inst.ID, "err", err)
		data, merr := json.Marshal(resp)
		if merr != nil {
			data = nil
		}
		req.Error("024", validate.CapDescription(fmt.Sprintf("instance %s: %v", inst.ID, err)), data)
	}
}

// waitJetStream asks the nodes for their JetStream state over a system
// account connection to one of them, in rounds until every node is ready,
// timeout passes, every node refuses the system credentials, the instance is
// no longer registered or the service closes. A close ends a dial or a round
// in progress. It returns nil when every node is ready, an error wrapping
// errInstanceDestroyed when the instance went, and one wrapping
// errJetStreamNotReady, naming each node not ready and why, otherwise.
func (s *Service) waitJetStream(instanceID string, timeout time.Duration, dial waitDial, nodes []waitNode) error {
	deadline := time.Now().Add(timeout)

	// ctx ends when the service closes, or when the wait returns.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-s.closing:
			cancel()
		case <-ctx.Done():
		}
	}()

	var nc *nats.Conn
	defer func() {
		if nc != nil {
			nc.Close()
		}
	}()

	var reasons map[string]string
	var dialErr error

	closed := func() error {
		return notReadyError("when the service closed", nodes, reasons, nc == nil, dialErr)
	}

	for {
		if ctx.Err() != nil {
			return closed()
		}
		if !s.registered(instanceID) {
			return errInstanceDestroyed
		}

		if nc == nil || nc.IsClosed() {
			var refused bool
			var err error
			nc, refused, err = dialAnyNode(ctx, dial, nodes, deadline)
			dialErr = moreInformative(dialErr, err)

			// Every node refused the credentials: no later round can do
			// better, so the wait ends now rather than when it times out.
			if refused {
				reasons = map[string]string{}
				for _, n := range nodes {
					reasons[n.name] = "refused the system credentials"
				}
				return notReadyError("as every server refused the system credentials", nodes, reasons, true, dialErr)
			}
		}

		if nc != nil {
			replies, err := gatherJSZ(ctx, nc, nodes, min(jszGatherWindow, max(time.Until(deadline), 0)))
			if err != nil {
				nc.Close()
				nc = nil
				dialErr = moreInformative(dialErr, err)
			}
			reasons = jetStreamNotReady(nodes, replies)
			if len(reasons) == 0 {
				return nil
			}
		} else {
			reasons = map[string]string{}
			for _, n := range nodes {
				reasons[n.name] = "no reply"
			}
		}

		if ctx.Err() != nil {
			return closed()
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return notReadyError(fmt.Sprintf("after %v", timeout), nodes, reasons, nc == nil, dialErr)
		}

		select {
		case <-ctx.Done():
			return closed()
		case <-time.After(min(jetStreamWaitInterval, remaining)):
		}
	}
}

// registered reports whether the instance is still registered.
func (s *Service) registered(instanceID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.instances[instanceID]
	return ok
}

// dialAnyNode dials the nodes in order and returns the first connection. When
// none could be dialed before deadline or ctx ended, it returns the most
// informative of their errors, and refused when every node was dialed and
// refused the credentials.
func dialAnyNode(ctx context.Context, dial waitDial, nodes []waitNode, deadline time.Time) (*nats.Conn, bool, error) {
	var dialErr error
	refusals := 0
	for _, n := range nodes {
		remaining := time.Until(deadline)
		if remaining <= 0 || ctx.Err() != nil {
			break
		}

		nc, err := dial.connect(ctx, n, min(jetStreamWaitDialTimeout, remaining))
		if err == nil {
			return nc, false, nil
		}
		if isAuthError(err) {
			refusals++
		}
		dialErr = moreInformative(dialErr, fmt.Errorf("%s: %w", n.name, err))
	}

	return nil, len(nodes) > 0 && refusals == len(nodes), dialErr
}

// isAuthError reports whether err is a server refusing a connection's
// credentials.
func isAuthError(err error) bool {
	return errors.Is(err, nats.ErrAuthorization) || errors.Is(err, nats.ErrAuthExpired) || errors.Is(err, nats.ErrAuthRevoked)
}

// isTimeout reports whether err is a dial or a request that timed out, which
// a deadline cut short as often as a node failed it.
func isTimeout(err error) bool {
	var netErr net.Error
	return errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || (errors.As(err, &netErr) && netErr.Timeout())
}

// moreInformative returns next unless it is nil, or it is a timeout and prev
// is an error that is not, so a dial whose timeout the wait's deadline cut
// short never hides why an earlier dial failed.
func moreInformative(prev, next error) error {
	if next == nil {
		return prev
	}
	if prev != nil && isTimeout(next) && !isTimeout(prev) {
		return prev
	}

	return next
}

// gatherJSZ sends one JSZ ping on nc and gathers the replies of the nodes,
// keyed by server name, until each has replied, window passes or ctx ends.
// Replies from servers that are not among the nodes are left out. It fails
// only when the ping cannot be sent.
func gatherJSZ(ctx context.Context, nc *nats.Conn, nodes []waitNode, window time.Duration) (map[string]*server.ServerAPIJszResponse, error) {
	want := map[string]bool{}
	for _, n := range nodes {
		want[n.serverName] = true
	}

	// Room for a reply from every node twice over, so none is dropped as a
	// slow consumer's while the loop below reads.
	msgs := make(chan *nats.Msg, 2*len(nodes))
	sub, err := nc.ChanSubscribe(nc.NewInbox(), msgs)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	payload, err := json.Marshal(server.JSzOptions{})
	if err != nil {
		return nil, err
	}

	err = nc.PublishRequest(jszPingSubject, sub.Subject, payload)
	if err != nil {
		return nil, err
	}

	timer := time.NewTimer(window)
	defer timer.Stop()

	replies := map[string]*server.ServerAPIJszResponse{}
	for len(replies) < len(want) {
		var msg *nats.Msg
		select {
		case msg = <-msgs:
		case <-timer.C:
			return replies, nil
		case <-ctx.Done():
			return replies, nil
		}

		var reply server.ServerAPIJszResponse
		err = json.Unmarshal(msg.Data, &reply)
		if err != nil || reply.Server == nil || !want[reply.Server.Name] {
			continue
		}
		replies[reply.Server.Name] = &reply
	}

	return replies, nil
}

// jetStreamNotReady returns why each node whose JetStream is not ready is
// not, by node name, and an empty map when every node is ready: every node
// replied with JetStream enabled and names the same meta leader, a node that
// names itself, whose reply lists every other node as a current peer that is
// not offline. A lone node runs JetStream without a meta group and is ready
// once it replies with JetStream enabled.
func jetStreamNotReady(nodes []waitNode, replies map[string]*server.ServerAPIJszResponse) map[string]string {
	reasons := map[string]string{}

	var meta []waitNode
	for _, n := range nodes {
		r := replies[n.serverName]
		switch {
		case r == nil:
			reasons[n.name] = "no reply"
		case r.Error != nil:
			reasons[n.name] = "replied with error: " + r.Error.Description
		case r.Data == nil || r.Data.Disabled:
			reasons[n.name] = "JetStream not enabled"
		case r.Data.Meta == nil && len(nodes) == 1:
		case r.Data.Meta == nil || r.Data.Meta.Leader == "":
			reasons[n.name] = "no meta leader"
		default:
			meta = append(meta, n)
		}
	}

	// The leader is the one node that names itself leader. Two that do are
	// a stale view of an election still settling.
	var leader *server.MetaClusterInfo
	var leaderName string
	claims := 0
	for _, n := range meta {
		m := replies[n.serverName].Data.Meta
		if m.Leader == n.serverName {
			leader, leaderName = m, n.serverName
			claims++
		}
	}
	if claims != 1 {
		leader, leaderName = nil, ""
	}

	for _, n := range meta {
		seen := replies[n.serverName].Data.Meta.Leader

		switch {
		case leader == nil:
			reasons[n.name] = fmt.Sprintf("meta leader %s not confirmed", seen)
		case seen != leaderName:
			reasons[n.name] = fmt.Sprintf("sees meta leader %s, not %s", seen, leaderName)
		case n.serverName == leaderName:
		default:
			reason := peerNotReady(leader, leaderName, n.serverName)
			if reason != "" {
				reasons[n.name] = reason
			}
		}
	}

	return reasons
}

// peerNotReady returns why the meta leader named leaderName, whose view is
// leader, does not hold the server named serverName as a ready peer, or empty
// when it does.
func peerNotReady(leader *server.MetaClusterInfo, leaderName, serverName string) string {
	for _, p := range leader.Replicas {
		if p == nil || p.Name != serverName {
			continue
		}

		switch {
		case p.Offline:
			return "offline on meta leader " + leaderName
		case !p.Current:
			return "not current on meta leader " + leaderName
		default:
			return ""
		}
	}

	return "not a peer of meta leader " + leaderName
}

// notReadyError returns the error a wait that gave up when returns, naming
// each node not ready, in node order, with its reason. When the wait held no
// connection at the end it adds the last dial error.
func notReadyError(when string, nodes []waitNode, reasons map[string]string, disconnected bool, dialErr error) error {
	var parts []string
	for _, n := range nodes {
		reason, ok := reasons[n.name]
		if ok {
			parts = append(parts, n.name+": "+reason)
		}
	}

	var dialed string
	if disconnected && dialErr != nil {
		dialed = fmt.Sprintf(" (could not connect: %v)", dialErr)
	}

	return fmt.Errorf("%w %s: %s%s", errJetStreamNotReady, when, strings.Join(parts, "; "), dialed)
}
