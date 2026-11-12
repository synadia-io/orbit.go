package ntf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/ntf/api"
)

// TestJetStreamWaitOptions pins what WithJetStreamWait and
// WithSystemCredentials set, and the request timeout a create with them gets.
func TestJetStreamWaitOptions(t *testing.T) {
	cases := []struct {
		name        string
		opts        []CreateOption
		wantSeconds float64
		wantCreds   *api.Credential
		wantTimeout time.Duration
	}{
		{name: "no wait", wantTimeout: createTimeout},
		{name: "whole seconds", opts: []CreateOption{WithJetStreamWait(30 * time.Second)}, wantSeconds: 30, wantTimeout: createTimeout + 30*time.Second + jetStreamWaitMargin},
		{name: "fractions", opts: []CreateOption{WithJetStreamWait(1500 * time.Millisecond)}, wantSeconds: 1.5, wantTimeout: createTimeout + 1500*time.Millisecond + jetStreamWaitMargin},
		{name: "zero is no wait", opts: []CreateOption{WithJetStreamWait(0)}, wantTimeout: createTimeout},
		{name: "negative is no wait", opts: []CreateOption{WithJetStreamWait(-time.Second)}, wantTimeout: createTimeout},
		{name: "zero undoes an earlier wait", opts: []CreateOption{WithJetStreamWait(time.Second), WithJetStreamWait(0)}, wantTimeout: createTimeout},
		{name: "later wait replaces", opts: []CreateOption{WithJetStreamWait(time.Second), WithJetStreamWait(2 * time.Second)}, wantSeconds: 2, wantTimeout: createTimeout + 2*time.Second + jetStreamWaitMargin},
		{name: "credentials", opts: []CreateOption{WithJetStreamWait(time.Second), WithSystemCredentials("admin", "s3cret")}, wantSeconds: 1,
			wantCreds: &api.Credential{UserPassword: &api.UserPassword{User: "admin", Password: "s3cret"}}, wantTimeout: createTimeout + time.Second + jetStreamWaitMargin},
		{name: "later credentials replace", opts: []CreateOption{WithSystemCredentials("a", "b"), WithSystemCredentials("admin", "")},
			wantCreds: &api.Credential{UserPassword: &api.UserPassword{User: "admin"}}, wantTimeout: createTimeout},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			co := resolveCreateOptions(tc.opts)
			if got := co.waitSeconds(); got != tc.wantSeconds {
				t.Errorf("waitSeconds() = %v, want %v", got, tc.wantSeconds)
			}
			if got := co.createRequestTimeout(); got != tc.wantTimeout {
				t.Errorf("createRequestTimeout() = %v, want %v", got, tc.wantTimeout)
			}
			got, _ := json.Marshal(co.systemCredentials)
			want, _ := json.Marshal(tc.wantCreds)
			if string(got) != string(want) {
				t.Errorf("systemCredentials = %s, want %s", got, want)
			}
		})
	}
}

// TestCreateRequestsCarryJetStreamWait pins that every kind of create puts the
// wait and the credentials on the wire, and leaves both out without them.
func TestCreateRequestsCarryJetStreamWait(t *testing.T) {
	f, m := startFakeService(t, func(string, map[string]any) (string, string, any) {
		return "", "", api.CreateResponse{ID: "fake"}
	})
	ctx := context.Background()
	wait := []CreateOption{WithJetStreamWait(2500 * time.Millisecond), WithSystemCredentials("admin", "s3cret")}

	creates := []func(opts ...CreateOption) (*Instance, error){
		func(opts ...CreateOption) (*Instance, error) { return m.CreateServer(ctx, true, opts...) },
		func(opts ...CreateOption) (*Instance, error) { return m.CreateCluster(ctx, 3, true, opts...) },
		func(opts ...CreateOption) (*Instance, error) { return m.CreateSuperCluster(ctx, 2, 2, true, opts...) },
	}
	for _, create := range creates {
		_, err := create(wait...)
		if err != nil {
			t.Fatalf("create with wait: %v", err)
		}
		_, err = create()
		if err != nil {
			t.Fatalf("create without wait: %v", err)
		}
	}

	got := f.seen()
	if len(got) != 2*len(creates) {
		t.Fatalf("the fake received %d requests, want %d", len(got), 2*len(creates))
	}
	for i := range len(creates) {
		waited, plain := got[2*i], got[2*i+1]
		if waited["wait_jetstream"] != 2.5 {
			t.Errorf("request %d wait_jetstream = %v, want 2.5", i, waited["wait_jetstream"])
		}
		creds, _ := json.Marshal(waited["system_credentials"])
		if string(creds) != `{"user_password":{"password":"s3cret","user":"admin"}}` {
			t.Errorf("request %d system_credentials = %s", i, creds)
		}
		for _, key := range []string{"wait_jetstream", "system_credentials"} {
			if _, ok := plain[key]; ok {
				t.Errorf("request %d without a wait = %v, want no %s", i, plain, key)
			}
		}
	}
}

// TestJetStreamNotReadyReturnsInstance pins how a create maps the service's
// 024: an error matching ErrJetStreamNotReady and ErrService that keeps the
// service's reason, with the instance from the refusal's data. A 024 without
// data, and a 404 from a destroy during the wait, return no instance.
func TestJetStreamNotReadyReturnsInstance(t *testing.T) {
	left := api.CreateResponse{ID: "inst-7", Kind: kindCluster, Servers: []*api.ManagedServer{{Name: "n1", Cluster: "c1", Port: 4242}}}
	const reason = "instance inst-7: JetStream not ready when the wait ended: n1: no meta leader"

	cases := []struct {
		name     string
		code     string
		desc     string
		data     any
		wantErrs []error
		wantInst bool
	}{
		{name: "024 with the instance", code: "024", desc: reason, data: left, wantErrs: []error{ErrService, ErrJetStreamNotReady}, wantInst: true},
		{name: "024 without data", code: "024", desc: reason, wantErrs: []error{ErrService, ErrJetStreamNotReady}},
		{name: "destroyed during the wait", code: "404", desc: "Instance destroyed while waiting for JetStream", wantErrs: []error{ErrService, ErrInstanceNotFound}},
		{name: "other refusals carry no instance", code: "011", desc: "wait_jetstream needs jetstream", data: left, wantErrs: []error{ErrService}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, m := startFakeService(t, func(string, map[string]any) (string, string, any) {
				return tc.code, tc.desc, tc.data
			})

			inst, err := m.CreateCluster(context.Background(), 3, true, WithJetStreamWait(time.Second))
			for _, want := range tc.wantErrs {
				if !errors.Is(err, want) {
					t.Errorf("err = %v, want it to match %v", err, want)
				}
			}
			if tc.code != "024" && errors.Is(err, ErrJetStreamNotReady) {
				t.Errorf("err = %v, want it not to match ErrJetStreamNotReady", err)
			}
			if err == nil || !strings.Contains(err.Error(), tc.desc) {
				t.Errorf("err = %v, want it to carry %q", err, tc.desc)
			}

			if !tc.wantInst {
				if inst != nil {
					t.Fatalf("inst = %+v, want nil", inst)
				}
				return
			}
			if inst == nil {
				t.Fatal("inst = nil, want the instance left in place")
			}
			if inst.ID != left.ID || inst.Kind != left.Kind || len(inst.Servers) != 1 {
				t.Fatalf("inst = %+v, want %+v", inst, left)
			}
			if !strings.HasSuffix(inst.Servers[0].URL, ":4242") {
				t.Errorf("server URL = %q, want it dialable on port 4242", inst.Servers[0].URL)
			}
			if inst.m != m {
				t.Error("inst is not bound to the Manager, so it cannot be destroyed through it")
			}
		})
	}
}

// TestCreateWaitsForJetStream proves an instance created with a JetStream
// wait takes JetStream requests at once: an account info on every kind of
// instance, and an R3 stream on a three node cluster, with no retry.
func TestCreateWaitsForJetStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	m, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(m.Close)

	cases := []struct {
		name     string
		replicas int
		create   func(opts ...CreateOption) (*Instance, error)
	}{
		{name: "server", replicas: 1, create: func(opts ...CreateOption) (*Instance, error) { return m.CreateServer(ctx, true, opts...) }},
		{name: "cluster", replicas: 3, create: func(opts ...CreateOption) (*Instance, error) { return m.CreateCluster(ctx, 3, true, opts...) }},
		{name: "super-cluster", replicas: 2, create: func(opts ...CreateOption) (*Instance, error) {
			return m.CreateSuperCluster(ctx, 2, 2, true, opts...)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inst, err := tc.create(WithDescription(t.Name()), WithJetStreamWait(60*time.Second))
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			t.Cleanup(func() { inst.Destroy(t) })

			nc, err := nats.Connect(inst.RandomServer().URL)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer nc.Close()

			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatalf("jetstream: %v", err)
			}

			rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
			defer rcancel()

			_, err = js.AccountInfo(rctx)
			if err != nil {
				t.Fatalf("account info right after the create: %v", err)
			}

			stream, err := js.CreateStream(rctx, jetstream.StreamConfig{Name: "WAITED", Subjects: []string{"waited"}, Replicas: tc.replicas})
			if err != nil {
				t.Fatalf("create R%d stream right after the create: %v", tc.replicas, err)
			}
			if got := stream.CachedInfo().Config.Replicas; got != tc.replicas {
				t.Fatalf("stream replicas = %d, want %d", got, tc.replicas)
			}
		})
	}
}

// TestCreateJetStreamWaitNotReady proves a wait that ends unready returns
// ErrJetStreamNotReady with the instance the service left in place, which is
// still there and can be destroyed: a wait too short for a meta leader to be
// elected, and system credentials every server refuses, which end a long wait
// at once.
func TestCreateJetStreamWaitNotReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	m, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(m.Close)

	cases := []struct {
		name     string
		opts     []CreateOption
		wantText string
		within   time.Duration
	}{
		{name: "wait too short", opts: []CreateOption{WithJetStreamWait(time.Millisecond)}, wantText: "JetStream not ready", within: time.Minute},
		{name: "refused system credentials", opts: []CreateOption{WithJetStreamWait(60 * time.Second), WithSystemCredentials("system", "wrong")},
			wantText: "refused the system credentials", within: 30 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			inst, err := m.CreateCluster(ctx, 3, true, append(tc.opts, WithDescription(t.Name()))...)
			took := time.Since(start)
			if inst != nil {
				t.Cleanup(func() { _, _ = m.Destroy(context.Background(), inst.ID) })
			}

			if !errors.Is(err, ErrJetStreamNotReady) || !errors.Is(err, ErrService) {
				t.Fatalf("err = %v, want ErrJetStreamNotReady and ErrService", err)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("err = %v, want the service's reason %q", err, tc.wantText)
			}
			if took > tc.within {
				t.Errorf("the create returned after %v, want within %v", took, tc.within)
			}

			if inst == nil {
				t.Fatal("inst = nil, want the instance left in place")
			}
			if len(inst.Servers) != 3 || inst.Kind != kindCluster {
				t.Fatalf("inst = %+v, want a cluster of 3 servers", inst)
			}
			if !strings.Contains(err.Error(), inst.ID) {
				t.Errorf("err = %v, want it to name instance %s", err, inst.ID)
			}

			_, err = m.InstanceStatus(ctx, inst.ID)
			if err != nil {
				t.Fatalf("status of the instance left in place: %v", err)
			}

			destroyed, err := m.Destroy(ctx, inst.ID)
			if err != nil || !destroyed.Destroyed {
				t.Fatalf("destroy of the instance left in place = %+v, %v", destroyed, err)
			}
		})
	}
}

// TestJetStreamWaitRefused proves the refusals the service gives a wait it
// cannot run surface as ErrService with no instance: a wait without
// JetStream, credentials without a wait, an accounts snippet without
// credentials, and a wait above the service's limit.
func TestJetStreamWaitRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m, err := Connect(ctx, testServerURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(m.Close)

	cases := []struct {
		name     string
		js       bool
		opts     []CreateOption
		wantText string
	}{
		{name: "without JetStream", opts: []CreateOption{WithJetStreamWait(5 * time.Second)}, wantText: "wait_jetstream needs jetstream"},
		{name: "credentials without a wait", js: true, opts: []CreateOption{WithSystemCredentials("admin", "s3cret")}, wantText: "system_credentials needs wait_jetstream"},
		{name: "accounts without credentials", js: true, opts: []CreateOption{WithJetStreamWait(5 * time.Second), WithAccounts("USERS1: {users: [{user: user1, password: password}], jetstream: enabled}")},
			wantText: "needs system_credentials"},
		{name: "wait above the limit", js: true, opts: []CreateOption{WithJetStreamWait(301 * time.Second)}, wantText: "wait_jetstream"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inst, err := m.CreateServer(ctx, tc.js, append(tc.opts, WithDescription(t.Name()))...)
			if inst != nil {
				_, _ = m.Destroy(context.Background(), inst.ID)
				t.Fatalf("inst = %+v, want none", inst)
			}
			if !errors.Is(err, ErrService) || errors.Is(err, ErrJetStreamNotReady) {
				t.Fatalf("err = %v, want ErrService and not ErrJetStreamNotReady", err)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Errorf("err = %v, want it to carry %q", err, tc.wantText)
			}
		})
	}
}

// fatalRecorder is a testing.TB whose Fatalf records the failure and ends the
// goroutine, so a test can call a Client method it expects to fail.
type fatalRecorder struct {
	testing.TB

	mu    sync.Mutex
	errs  []string
	fatal string
}

func (r *fatalRecorder) Helper() {}

func (r *fatalRecorder) Name() string { return "fatalRecorder" }

func (r *fatalRecorder) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func (r *fatalRecorder) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.fatal = fmt.Sprintf(format, args...)
	r.mu.Unlock()
	runtime.Goexit()
}

// TestClientCreateDestroysInstanceNotReady proves a Client create whose
// JetStream wait ends unready destroys the instance the service left in place
// before failing the test, so it does not outlive the test.
func TestClientCreateDestroysInstanceNotReady(t *testing.T) {
	client := New(t, testServerURL())
	t.Cleanup(func() { client.Close(t) })

	before := client.List(t)
	known := map[string]bool{}
	for _, inst := range before.Instances {
		known[inst.ID] = true
	}

	rec := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.CreateCluster(rec, 3, true, WithJetStreamWait(time.Millisecond), WithDescription(t.Name()))
	}()
	<-done

	if !strings.Contains(rec.fatal, "could not create cluster") || !strings.Contains(rec.fatal, ErrJetStreamNotReady.Error()) {
		t.Fatalf("the create failed with %q, want ErrJetStreamNotReady", rec.fatal)
	}
	if len(rec.errs) != 0 {
		t.Fatalf("the create reported %v", rec.errs)
	}

	for _, inst := range client.List(t).Instances {
		if !known[inst.ID] && inst.Description == t.Name() {
			t.Fatalf("instance %s left by the failed create is still there", inst.ID)
		}
	}
}
