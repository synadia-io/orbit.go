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

func TestIpqueuesz(t *testing.T) {
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
		options   natssysclient.IpqueueszEventOptions
		withError error
	}{
		{
			name: "with valid id",
			id:   c.servers[1].ID(),
			options: natssysclient.IpqueueszEventOptions{
				IpqueueszOptions: natssysclient.IpqueueszOptions{All: true},
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
			ipqueuesz, err := sys.Ipqueuesz(ctx, test.id, test.options)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error; want: %s; got: %s", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unable to fetch IPQUEUESZ: %s", err)
			}
			if ipqueuesz.Server.ID != test.id {
				t.Fatalf("Invalid server IPQUEUESZ response: %+v", ipqueuesz)
			}
			if len(ipqueuesz.Ipqueuesz) == 0 {
				t.Fatalf("Expected at least one internal queue when All=true")
			}
		})
	}
}

func TestIpqueueszFilter(t *testing.T) {
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

	serverID := c.servers[1].ID()

	all, err := sys.Ipqueuesz(ctx, serverID, natssysclient.IpqueueszEventOptions{
		IpqueueszOptions: natssysclient.IpqueueszOptions{All: true},
	})
	if err != nil {
		t.Fatalf("Unable to fetch IPQUEUESZ: %s", err)
	}
	if len(all.Ipqueuesz) == 0 {
		t.Fatalf("Expected at least one queue when All=true")
	}

	var probe string
	for name := range all.Ipqueuesz {
		probe = name
		break
	}
	if len(probe) < 2 {
		t.Fatalf("Unexpected queue name %q used as filter probe", probe)
	}
	fragment := probe[:len(probe)/2]

	filtered, err := sys.Ipqueuesz(ctx, serverID, natssysclient.IpqueueszEventOptions{
		IpqueueszOptions: natssysclient.IpqueueszOptions{All: true, Filter: fragment},
	})
	if err != nil {
		t.Fatalf("Unable to fetch filtered IPQUEUESZ: %s", err)
	}
	if len(filtered.Ipqueuesz) == 0 {
		t.Fatalf("Expected at least one queue matching filter %q", fragment)
	}
	if len(filtered.Ipqueuesz) > len(all.Ipqueuesz) {
		t.Fatalf("Filtered result (%d) must not exceed unfiltered (%d)", len(filtered.Ipqueuesz), len(all.Ipqueuesz))
	}
	for name := range filtered.Ipqueuesz {
		if !strings.Contains(name, fragment) {
			t.Fatalf("Queue %q does not contain filter fragment %q", name, fragment)
		}
	}
}

func TestIpqueueszPing(t *testing.T) {
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
	resp, err := sys.IpqueueszPing(ctx, natssysclient.IpqueueszEventOptions{
		IpqueueszOptions: natssysclient.IpqueueszOptions{All: true},
	})
	if err != nil {
		t.Fatalf("Unable to fetch IPQUEUESZ: %s", err)
	}
	if len(resp) != 3 {
		t.Fatalf("Invalid number of responses: %d; want: %d", len(resp), 3)
	}
	for _, ipq := range resp {
		if len(ipq.Ipqueuesz) == 0 {
			t.Fatalf("Expected at least one internal queue for server %q", ipq.Server.ID)
		}
	}
}
