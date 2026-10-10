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

func TestAccountz(t *testing.T) {
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
		options   natssysclient.AccountzEventOptions
		withError error
	}{
		{
			name: "list accounts",
			id:   c.servers[1].ID(),
		},
		{
			name: "account detail",
			id:   c.servers[1].ID(),
			options: natssysclient.AccountzEventOptions{
				AccountzOptions: natssysclient.AccountzOptions{Account: "JS"},
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
			accountz, err := sys.Accountz(ctx, test.id, test.options)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error; want: %s; got: %s", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unable to fetch ACCOUNTZ: %s", err)
			}
			if accountz.Server.ID != test.id {
				t.Fatalf("Invalid server ACCOUNTZ response: %+v", accountz)
			}
			if test.options.Account == "" {
				if len(accountz.Accountz.Accounts) == 0 {
					t.Fatalf("Expected non-empty account list")
				}
				return
			}
			if accountz.Accountz.Account == nil {
				t.Fatalf("Expected account details in the response")
			}
			if accountz.Accountz.Account.AccountName != test.options.Account {
				t.Fatalf("Invalid account name: got %q, want %q", accountz.Accountz.Account.AccountName, test.options.Account)
			}
			if !accountz.Accountz.Account.JetStream {
				t.Fatalf("Expected JetStream=true for account %q", test.options.Account)
			}
			if accountz.Accountz.Account.IsSystem {
				t.Fatalf("Expected IsSystem=false for account %q", test.options.Account)
			}
		})
	}
}

func TestAccountzPing(t *testing.T) {
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
	resp, err := sys.AccountzPing(ctx, natssysclient.AccountzEventOptions{})
	if err != nil {
		t.Fatalf("Unable to fetch ACCOUNTZ: %s", err)
	}
	if len(resp) != 3 {
		t.Fatalf("Invalid number of responses: %d; want: %d", len(resp), 3)
	}
	for _, accountz := range resp {
		if len(accountz.Accountz.Accounts) == 0 {
			t.Fatalf("Expected non-empty account list for server %q", accountz.Server.ID)
		}
	}
}
