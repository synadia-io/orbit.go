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
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var configFiles = []string{
	"./testdata/s1.conf",
	"./testdata/s2.conf",
	"./testdata/s3.conf",
}

type Cluster struct {
	servers []*server.Server
}

// StartJetStreamServer starts a NATS server
func StartJetStreamServer(t *testing.T, confFile string) *server.Server {
	opts, err := server.ProcessConfigFile(confFile)

	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	opts.NoLog = true
	opts.StoreDir = t.TempDir()
	opts.Port = -1

	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Error creating server: %s", err)
	}

	s.Start()

	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("Unable to start NATS Server")
	}
	return s
}

func SetupCluster(t *testing.T) *Cluster {
	t.Helper()
	cluster := Cluster{
		servers: make([]*server.Server, 0),
	}
	for _, confFile := range configFiles {
		srv := StartJetStreamServer(t, confFile)
		cluster.servers = append(cluster.servers, srv)
	}
	connections := make([]*nats.Conn, 0, len(cluster.servers))
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	for _, s := range cluster.servers {
		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("Unable to connect to server %q: %s", s.Name(), err)
		}
		connections = append(connections, nc)

		// wait until JetStream is ready
		timeout := time.Now().Add(10 * time.Second)
		for time.Now().Before(timeout) {
			jsm, err := nc.JetStream()
			if err != nil {
				t.Fatal(err)
			}
			_, err = jsm.AccountInfo()
			if err != nil {
				time.Sleep(500 * time.Millisecond)
			}
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}
	}
	return &cluster
}

func (c *Cluster) Shutdown() {
	for _, s := range c.servers {
		s.Shutdown()
	}
}
