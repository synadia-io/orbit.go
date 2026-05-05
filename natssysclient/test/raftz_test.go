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
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-io/orbit.go/natssysclient"
)

func TestRaftz(t *testing.T) {
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

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			raftz, err := sys.Raftz(ctx, test.id, natssysclient.RaftzEventOptions{})
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error; want: %s; got: %s", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unable to fetch RAFTZ: %s", err)
			}
			if raftz.Server.ID != test.id {
				t.Fatalf("Invalid server RAFTZ response: %+v", raftz)
			}
			if len(raftz.Raftz) == 0 {
				t.Fatalf("Expected at least one Raft group in a JetStream cluster")
			}
		})
	}
}

func TestRaftzAccountFilter(t *testing.T) {
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

	// Create a replicated stream in the JS account so it owns at least one raft group.
	jsConn, err := nats.Connect(strings.Join(urls, ","))
	if err != nil {
		t.Fatalf("Error connecting JS client: %s", err)
	}
	defer jsConn.Close()
	js, err := jsConn.JetStream()
	if err != nil {
		t.Fatalf("Error getting JetStream context: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "raftz_test", Subjects: []string{"raftz.test"}, Replicas: 3}); err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}

	sys, err := natssysclient.NewSysClient(sysConn)
	if err != nil {
		t.Fatalf("Error creating system client: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Wait for the stream's raft group to appear in RAFTZ for the JS account.
	deadline := time.Now().Add(5 * time.Second)
	var filtered *natssysclient.RaftzResp
	for time.Now().Before(deadline) {
		filtered, err = sys.Raftz(ctx, c.servers[0].ID(), natssysclient.RaftzEventOptions{
			RaftzOptions: natssysclient.RaftzOptions{AccountFilter: "JS"},
		})
		if err != nil {
			t.Fatalf("Unable to fetch RAFTZ: %s", err)
		}
		if len(filtered.Raftz["JS"]) > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if len(filtered.Raftz) == 0 {
		t.Fatalf("Expected at least one account key with AccountFilter=JS, got none")
	}
	for acct := range filtered.Raftz {
		if acct != "JS" {
			t.Fatalf("AccountFilter=JS returned entry for account %q", acct)
		}
	}
	if len(filtered.Raftz["JS"]) == 0 {
		t.Fatalf("Expected at least one raft group in JS account after creating a replicated stream")
	}
}

func TestRaftzPing(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := sys.RaftzPing(ctx, natssysclient.RaftzEventOptions{})
	if err != nil {
		t.Fatalf("Unable to fetch RAFTZ: %s", err)
	}
	if len(resp) != 3 {
		t.Fatalf("Invalid number of responses: %d; want: %d", len(resp), 3)
	}
	for _, raftz := range resp {
		if len(raftz.Raftz) == 0 {
			t.Fatalf("Expected at least one Raft group for server %q", raftz.Server.ID)
		}
	}
}
