// Copyright 2024 Synadia Communications Inc.
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

package test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/natssysclient"
)

func TestSubsz(t *testing.T) {
	c := SetupCluster(t)
	defer c.Shutdown()

	if len(c.servers) != 3 {
		t.Fatalf("Unexpected number of servers started: %d; want: %d", len(c.servers), 3)
	}

	var urls []string
	for _, s := range c.servers {
		urls = append(urls, s.ClientURL())
	}

	sysConn, err := nats.Connect(strings.Join(urls, ","), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Error establishing connection: %s", err)
	}

	nc, err := nats.Connect(c.servers[1].ClientURL())
	if err != nil {
		t.Fatalf("Error establishing connection: %s", err)
	}
	sub, err := nc.Subscribe("foo", func(msg *nats.Msg) {})
	if err != nil {
		t.Fatalf("Error establishing connection: %s", err)
	}
	time.Sleep(100 * time.Millisecond)
	defer sub.Unsubscribe()

	tests := []struct {
		name      string
		id        string
		withError error
	}{
		{
			name: "with valid id",
			id:   c.servers[1].ID(),
		},
		{
			name:      "with empty id",
			id:        "",
			withError: natssysclient.ErrValidation,
		},
		{
			name:      "with invalid id",
			id:        "asd",
			withError: natssysclient.ErrInvalidServerID,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sys, err := natssysclient.NewSysClient(sysConn)
			if err != nil {
				t.Fatalf("Error creating system client: %s", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			subsz, err := sys.ServerSubsz(ctx, test.id, natssysclient.SubszOptions{Subscriptions: true})
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error; want: %s; got: %s", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unable to fetch SUBSZ: %s", err)
			}
			if subsz.Subsz.ID != test.id {
				t.Fatalf("Invalid server SUBSZ response: %+v", subsz)
			}

			var found bool
			for _, sub := range subsz.Subsz.Subs {
				if sub.Subject == "foo" {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("Expected to find subscription on %q in the response, got none", "foo")
			}
		})
	}
}

func TestSubszPing(t *testing.T) {
	c := SetupCluster(t)
	defer c.Shutdown()

	if len(c.servers) != 3 {
		t.Fatalf("Unexpected number of servers started: %d; want: %d", len(c.servers), 3)
	}

	var urls []string
	for _, s := range c.servers {
		urls = append(urls, s.ClientURL())
	}

	sysConn, err := nats.Connect(strings.Join(urls, ","), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Error establishing connection: %s", err)
	}

	sys, err := natssysclient.NewSysClient(sysConn)
	if err != nil {
		t.Fatalf("Error creating system client: %s", err)
	}
	nc, err := nats.Connect(c.servers[1].ClientURL())
	if err != nil {
		t.Fatalf("Error establishing connection: %s", err)
	}
	sub, err := nc.Subscribe("foo", func(msg *nats.Msg) {})
	if err != nil {
		t.Fatalf("Error creating subscription: %s", err)
	}
	time.Sleep(3 * time.Second)
	defer sub.Unsubscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := sys.ServerSubszPing(ctx, natssysclient.SubszOptions{Subscriptions: true})
	if err != nil {
		t.Fatalf("Unable to fetch SUBSZ: %s", err)
	}
	if len(resp) != 3 {
		t.Fatalf("Invalid number of responses: %d; want: %d", len(resp), 3)
	}
	for _, s := range c.servers {
		var seen bool
		for _, subsz := range resp {
			if s.ID() == subsz.Subsz.ID {
				seen = true
				break
			}
		}
		if !seen {
			t.Fatalf("Expected server %q in the response", s.Name())
		}
	}
}
