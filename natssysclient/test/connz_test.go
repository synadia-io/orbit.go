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

func TestConnz(t *testing.T) {
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
			connz, err := sys.Connz(ctx, test.id, natssysclient.ConnzEventOptions{})
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error; want: %s; got: %s", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unable to fetch CONNZ: %s", err)
			}
			if connz.Connz.ID != test.id {
				t.Fatalf("Invalid server CONNZ response: %+v", connz)
			}
		})
	}
}

func TestConnzPing(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := sys.ConnzPing(ctx, natssysclient.ConnzEventOptions{})
	if err != nil {
		t.Fatalf("Unable to fetch CONNZ: %s", err)
	}
	if len(resp) != 3 {
		t.Fatalf("Invalid number of responses: %d; want: %d", len(resp), 3)
	}
	for _, s := range c.servers {
		var seen bool
		for _, connz := range resp {
			if s.ID() == connz.Connz.ID {
				seen = true
				break
			}
		}
		if !seen {
			t.Fatalf("Expected server %q in the response", s.Name())
		}
	}
}

func TestAllConnzPagination(t *testing.T) {
	c := SetupCluster(t)
	defer c.Shutdown()

	var urls []string
	for _, s := range c.servers {
		urls = append(urls, s.ClientURL())
	}

	sysConn, err := nats.Connect(urls[2], nats.UserInfo("admin", "s3cr3t!"))
	if err != nil {
		t.Fatalf("Error establishing connection: %s", err)
	}

	var testConns []*nats.Conn
	defer func() {
		for _, conn := range testConns {
			conn.Close()
		}
	}()

	for i := range 15 {
		nc, err := nats.Connect(c.servers[0].ClientURL())
		if err != nil {
			t.Fatalf("Error creating test connection %d: %s", i, err)
		}
		testConns = append(testConns, nc)
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
		opts := natssysclient.ConnzEventOptions{
			ConnzOptions: natssysclient.ConnzOptions{
				Limit: 5,
			},
		}

		var totalConns int
		var responses []*natssysclient.ConnzResp

		for resp, err := range sys.AllConnz(ctx, serverID, opts) {
			if err != nil {
				t.Fatalf("Error during pagination: %s", err)
			}
			responses = append(responses, resp)
			totalConns += len(resp.Connz.Conns)
		}

		// 15 connections on s1 with limit=5 should yield 3 pages
		expectedResponses := 3
		if len(responses) != expectedResponses {
			t.Errorf("Expected at least 3 responses with limit=5 and 15+ connections, got %d", len(responses))
		}

		firstResp := responses[0]
		if totalConns != firstResp.Connz.Total {
			t.Errorf("Total connections mismatch: got %d, want %d", totalConns, firstResp.Connz.Total)
		}
	})

	t.Run("ping all servers pagination", func(t *testing.T) {
		opts := natssysclient.ConnzEventOptions{
			ConnzOptions: natssysclient.ConnzOptions{
				Limit: 3,
			},
		}

		serverIterators, err := sys.AllConnzPing(ctx, opts)
		if err != nil {
			t.Fatalf("Error getting server iterators: %s", err)
		}

		if len(serverIterators) != len(c.servers) {
			t.Errorf("Expected %d iterators, got %d", len(c.servers), len(serverIterators))
		}

		var totalResponses int
		var totalConns int
		responsesByServer := make(map[string]int)

		for _, iter := range serverIterators {
			for resp, err := range iter {
				if err != nil {
					t.Fatalf("Error during ping pagination: %s", err)
				}
				totalResponses++
				totalConns += len(resp.Connz.Conns)
				responsesByServer[resp.Server.ID]++
			}
		}

		expectedResponses := 7 // 15 connections on s1 with limit=3 should yield at least 5 pages for s1 and 1 each for s2 and s3
		if totalResponses != expectedResponses {
			t.Errorf("Expected at least %d responses total, got %d", expectedResponses, totalResponses)
		}

		for serverID, count := range responsesByServer {
			if serverID == c.servers[0].ID() && count != 5 {
				t.Errorf("Server %s should have at least 3 paginated responses, got %d", serverID, count)
			}
		}
	})
}
