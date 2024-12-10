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

func TestJsz(t *testing.T) {
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
		options   natssysclient.JszEventOptions
		withError error
	}{
		{
			name: "with valid id",
			id:   c.servers[1].ID(),
		},
		{
			name: "with stream details",
			id:   c.servers[1].ID(),
			options: natssysclient.JszEventOptions{
				JszOptions: natssysclient.JszOptions{Accounts: true},
			},
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
			jsz, err := sys.Jsz(ctx, test.id, test.options)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error; want: %s; got: %s", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unable to fetch JSZ: %s", err)
			}
			if jsz.Server.ID != test.id {
				t.Fatalf("Invalid server JSZ response: %+v", jsz)
			}
			if !test.options.Accounts {
				if len(jsz.JSInfo.AccountDetails) != 0 {
					t.Fatalf("Expected no account details, got: %+v", jsz.JSInfo.AccountDetails)
				}
				return
			}
			if len(jsz.JSInfo.AccountDetails) == 0 {
				t.Fatalf("Expected account details in the response")
			}
		})
	}
}

func TestJszPing(t *testing.T) {
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

	nc, err := nats.Connect(strings.Join(urls, ","))
	if err != nil {
		t.Fatalf("Error establishing connection: %s", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Error getting JetStream context: %v", err)
	}
	_, err = js.AddStream(&nats.StreamConfig{Name: "s1", Subjects: []string{"foo"}})
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}

	sys, err := natssysclient.NewSysClient(sysConn)
	if err != nil {
		t.Fatalf("Error creating system client: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := sys.JszPing(ctx, natssysclient.JszEventOptions{JszOptions: natssysclient.JszOptions{Streams: true}})
	if err != nil {
		t.Fatalf("Unable to fetch JSZ: %s", err)
	}
	if len(resp) != 3 {
		t.Fatalf("Invalid number of responses: %d; want: %d", len(resp), 3)
	}
	var streamsNum int
	for _, s := range c.servers {
		var seen bool
		for _, jsz := range resp {
			if s.ID() == jsz.Server.ID {
				seen = true
			}
			streamsNum += len(jsz.JSInfo.AccountDetails[0].Streams)
		}
		if !seen {
			t.Fatalf("Expected server %q in the response", s.Name())
		}
		if streamsNum == 0 {
			t.Fatalf("Expected stream details in the response")
		}
	}
}
