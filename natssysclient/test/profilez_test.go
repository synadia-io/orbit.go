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

func TestProfilez(t *testing.T) {
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
		options   natssysclient.ProfilezEventOptions
		withError error
	}{
		{
			name: "heap profile",
			id:   c.servers[1].ID(),
			options: natssysclient.ProfilezEventOptions{
				ProfilezOptions: natssysclient.ProfilezOptions{Name: "heap"},
			},
		},
		{
			name: "goroutine profile",
			id:   c.servers[1].ID(),
			options: natssysclient.ProfilezEventOptions{
				ProfilezOptions: natssysclient.ProfilezOptions{Name: "goroutine", Debug: 1},
			},
		},
		{
			name:      "with empty id",
			id:        "",
			options:   natssysclient.ProfilezEventOptions{ProfilezOptions: natssysclient.ProfilezOptions{Name: "heap"}},
			withError: natssysclient.ErrValidation,
		},
		{
			name:      "with invalid id",
			id:        "asd",
			options:   natssysclient.ProfilezEventOptions{ProfilezOptions: natssysclient.ProfilezOptions{Name: "heap"}},
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
			profilez, err := sys.Profilez(ctx, test.id, test.options)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error; want: %s; got: %s", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unable to fetch PROFILEZ: %s", err)
			}
			if profilez.Server.ID != test.id {
				t.Fatalf("Invalid server PROFILEZ response: %+v", profilez)
			}
			if profilez.Profilez.Error != "" {
				t.Fatalf("Profilez returned error: %s", profilez.Profilez.Error)
			}
			if len(profilez.Profilez.Profile) == 0 {
				t.Fatalf("Expected non-empty profile data")
			}
		})
	}
}

func TestProfilezCPU(t *testing.T) {
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

	// CPU profile forces the server to sleep on the request path for Duration; budget generously.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := time.Now()
	profilez, err := sys.Profilez(ctx, c.servers[1].ID(), natssysclient.ProfilezEventOptions{
		ProfilezOptions: natssysclient.ProfilezOptions{Name: "cpu", Duration: 500 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("Unable to fetch PROFILEZ: %s", err)
	}
	elapsed := time.Since(start)

	if elapsed < 500*time.Millisecond {
		t.Fatalf("CPU profile returned too fast (%s); expected at least Duration to elapse", elapsed)
	}
	if profilez.Profilez.Error != "" {
		t.Fatalf("Profilez returned error: %s", profilez.Profilez.Error)
	}
	if len(profilez.Profilez.Profile) == 0 {
		t.Fatalf("Expected non-empty CPU profile data")
	}
}

func TestProfilezPing(t *testing.T) {
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
	resp, err := sys.ProfilezPing(ctx, natssysclient.ProfilezEventOptions{
		ProfilezOptions: natssysclient.ProfilezOptions{Name: "heap"},
	})
	if err != nil {
		t.Fatalf("Unable to fetch PROFILEZ: %s", err)
	}
	if len(resp) != 3 {
		t.Fatalf("Invalid number of responses: %d; want: %d", len(resp), 3)
	}
	for _, profilez := range resp {
		if profilez.Profilez.Error != "" {
			t.Fatalf("Profilez error for server %q: %s", profilez.Server.ID, profilez.Profilez.Error)
		}
		if len(profilez.Profilez.Profile) == 0 {
			t.Fatalf("Expected non-empty profile data for server %q", profilez.Server.ID)
		}
	}
}
