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

package test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/natssysclient"
)

func TestRoutez(t *testing.T) {
	c := SetupCluster(t)
	defer c.Shutdown()

	var urls []string
	for _, s := range c.servers {
		urls = append(urls, s.ClientURL())
	}

	sysConn, err := nats.Connect(strings.Join(urls, ","), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Error establishing connection: %s", err)
	}

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
			routez, err := sys.Routez(ctx, test.id, natssysclient.RoutezEventOptions{})
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error; want: %s; got: %s", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unable to fetch ROUTEZ: %s", err)
			}
			if routez.Server.ID != test.id {
				t.Fatalf("Invalid server ROUTEZ response: %+v", routez)
			}
			if routez.Routez.NumRoutes == 0 {
				t.Fatalf("Expected at least one route in a 3-node cluster")
			}
		})
	}
}

func TestRoutezSubscriptions(t *testing.T) {
	c := SetupCluster(t)
	defer c.Shutdown()

	var urls []string
	for _, s := range c.servers {
		urls = append(urls, s.ClientURL())
	}

	sysConn, err := nats.Connect(strings.Join(urls, ","), nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Error establishing connection: %s", err)
	}

	// A client subscription on any server should propagate to peers via the route protocol;
	// asking a peer server for ROUTEZ with Subscriptions: true should then surface that subject
	// in at least one of its route records.
	nc, err := nats.Connect(c.servers[0].ClientURL())
	if err != nil {
		t.Fatalf("Error connecting client: %s", err)
	}
	defer nc.Close()

	const subject = "test.routez.subs"
	if _, err := nc.SubscribeSync(subject); err != nil {
		t.Fatalf("Error subscribing: %s", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error flushing: %s", err)
	}

	sys, err := natssysclient.NewSysClient(sysConn)
	if err != nil {
		t.Fatalf("Error creating system client: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deadline := time.Now().Add(5 * time.Second)
	var found bool
	for time.Now().Before(deadline) {
		routez, err := sys.Routez(ctx, c.servers[1].ID(), natssysclient.RoutezEventOptions{
			RoutezOptions: natssysclient.RoutezOptions{Subscriptions: true},
		})
		if err != nil {
			t.Fatalf("Unable to fetch ROUTEZ: %s", err)
		}
		for _, route := range routez.Routez.Routes {
			if slices.Contains(route.Subs, subject) {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !found {
		t.Fatalf("Expected subject %q to appear in a route's Subs after propagation", subject)
	}
}

func TestRoutezPing(t *testing.T) {
	c := SetupCluster(t)
	defer c.Shutdown()

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := sys.RoutezPing(ctx, natssysclient.RoutezEventOptions{})
	if err != nil {
		t.Fatalf("Unable to fetch ROUTEZ: %s", err)
	}
	if len(resp) != 3 {
		t.Fatalf("Invalid number of responses: %d; want: %d", len(resp), 3)
	}
	for _, routez := range resp {
		if routez.Routez.NumRoutes == 0 {
			t.Fatalf("Expected at least one route per server in a 3-node cluster")
		}
	}
}
