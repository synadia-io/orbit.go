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
	"fmt"
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

func TestAllServerSubszPagination(t *testing.T) {
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

	var allConns []*nats.Conn
	defer func() {
		for _, conn := range allConns {
			conn.Close()
		}
	}()

	for i, server := range c.servers {
		nc, err := nats.Connect(server.ClientURL())
		if err != nil {
			t.Fatalf("Error establishing connection to server %d: %s", i, err)
		}
		allConns = append(allConns, nc)

		for j := range 10 {
			_, err := nc.Subscribe(fmt.Sprintf("server%d.sub%d", i, j), func(msg *nats.Msg) {})
			if err != nil {
				t.Fatalf("Error creating subscription %d on server %d: %s", j, i, err)
			}
		}
	}

	time.Sleep(200 * time.Millisecond)

	sys, err := natssysclient.NewSysClient(sysConn)
	if err != nil {
		t.Fatalf("Error creating system client: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	serverID := c.servers[0].ID()

	t.Run("single server pagination", func(t *testing.T) {
		t.Skip("Skipping due to server bug: https://github.com/nats-io/nats-server/pull/7009")

		opts := natssysclient.SubszOptions{
			Limit:         5,
			Subscriptions: true,
		}

		var totalSubs int
		var responses []*natssysclient.SubszResp

		for resp, err := range sys.AllServerSubsz(ctx, serverID, opts) {
			if err != nil {
				t.Fatalf("Error during pagination: %s", err)
			}
			responses = append(responses, resp)
			totalSubs += len(resp.Subsz.Subs)
		}

		if len(responses) < 2 {
			t.Errorf("Expected at least 2 responses with limit=5 and 10+ subscriptions, got %d", len(responses))
		}

		firstResp := responses[0]
		if totalSubs != firstResp.Subsz.Total {
			t.Errorf("Total subscriptions mismatch: got %d, want %d", totalSubs, firstResp.Subsz.Total)
		}
	})

	t.Run("ping all servers pagination", func(t *testing.T) {
		t.Skip("Skipping due to server bug: https://github.com/nats-io/nats-server/pull/7009")

		opts := natssysclient.SubszOptions{
			Limit:         3,
			Subscriptions: true,
		}

		serverIterators, err := sys.AllServerSubszPing(ctx, opts)
		if err != nil {
			t.Fatalf("Error getting server iterators: %s", err)
		}

		if len(serverIterators) != len(c.servers) {
			t.Errorf("Expected %d iterators, got %d", len(c.servers), len(serverIterators))
		}

		var totalResponses int
		var totalSubs int
		responsesByServer := make(map[string]int)

		for _, iter := range serverIterators {
			for resp, err := range iter {
				if err != nil {
					t.Fatalf("Error during ping pagination: %s", err)
				}
				totalResponses++
				totalSubs += len(resp.Subsz.Subs)
				responsesByServer[resp.Server.ID]++
			}
		}

		if totalResponses < len(c.servers)*3 {
			t.Errorf("Expected at least %d responses total, got %d", len(c.servers)*3, totalResponses)
		}

		for serverID, count := range responsesByServer {
			if count < 3 {
				t.Errorf("Server %s should have at least 3 paginated responses, got %d", serverID, count)
			}
		}
	})
}
