package ntf

import (
	"time"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// jetStreamWaitMargin is what a create given WithJetStreamWait allows on top
// of the wait and createTimeout for the service's answer to arrive: the wait
// ends its last round and closes its connection before the service answers.
const jetStreamWaitMargin = 5 * time.Second

// WithJetStreamWait makes a JetStream create return only once JetStream takes
// requests on the new instance, waiting at most timeout for it: every server
// reports JetStream enabled under the same meta leader, which lists every
// other server as a current peer. A lone server is ready once it reports
// JetStream enabled. Without it a create returns as soon as every server has
// started, before a cluster has elected its meta leader, so the first
// JetStream requests can fail with 10008 or time out. Sub-second durations are
// honored; the service takes at most 5 minutes. A non-positive duration turns
// the wait off, undoing an earlier WithJetStreamWait. It applies to every kind
// of create.
//
// The service asks each server for its JetStream state as the built-in
// template's system user. A create whose WithTemplate, WithAccounts or
// WithSystemAccount replaces that user must also give WithSystemCredentials,
// and one with a non-empty WithTLSSnippet cannot wait; the service refuses either with
// ErrService before anything starts, as it does a wait on a create without
// JetStream.
//
// The create's request timeout grows by the wait, so a context without a
// deadline waits long enough for the service's answer. A context with a
// deadline keeps it: give it more than the wait plus the time a create takes,
// or the create returns ErrRequest while the service still waits, and the
// instance it goes on to create stays on the service, unknown to the caller,
// until it is destroyed or the service is reset.
//
// When the wait ends with JetStream not ready, the create returns an error
// matching ErrJetStreamNotReady and ErrService, whose text names each server
// not ready and why. The service leaves the instance in place, and the
// create returns it alongside the error whenever the service's answer names
// it: the caller owns it and must destroy it. The Client create methods and
// helpers destroy it themselves before failing the test. A destroy or reset
// of the instance while the create waits normally ends the wait with
// ErrInstanceNotFound and no instance; one in the wait's last round can
// instead end it with ErrJetStreamNotReady and an instance already gone.
func WithJetStreamWait(timeout time.Duration) CreateOption {
	return createOpt(func(o *createOptions) {
		o.jetStreamWait = max(timeout, 0)
	})
}

// WithSystemCredentials gives the user and password that WithJetStreamWait
// authenticates to the instance's system account with, in place of the
// built-in template's system user. A create that replaces that user, with
// WithTemplate, WithAccounts or WithSystemAccount, needs them to wait. The
// service refuses them without WithJetStreamWait. Credentials the servers
// refuse end the wait at once with ErrJetStreamNotReady. A later
// WithSystemCredentials replaces an earlier one.
func WithSystemCredentials(user string, password string) CreateOption {
	return createOpt(func(o *createOptions) {
		o.systemCredentials = &api.Credential{UserPassword: &api.UserPassword{User: user, Password: password}}
	})
}

// waitSeconds is the wait_jetstream of a create with these options, in
// seconds, zero for no wait.
func (o createOptions) waitSeconds() float64 {
	return o.jetStreamWait.Seconds()
}

// createRequestTimeout is the timeout of a create request whose context has no
// deadline: createTimeout, plus the JetStream wait and jetStreamWaitMargin
// when it waits, so the request outlasts the wait.
func (o createOptions) createRequestTimeout() time.Duration {
	if o.jetStreamWait <= 0 {
		return createTimeout
	}
	return createTimeout + o.jetStreamWait + jetStreamWaitMargin
}
