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

package natssysclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type (
	// ProfilezResp is the response from a Profilez request.
	ProfilezResp struct {
		Server   ServerInfo     `json:"server"`
		Profilez ProfilezStatus `json:"data"`
		Error    APIError       `json:"error,omitempty"`
	}

	// ProfilezStatus contains a runtime profile produced by the server.
	ProfilezStatus struct {
		Profile []byte `json:"profile"`
		Error   string `json:"error"`
	}

	// ProfilezOptions are the options passed to Profilez.
	ProfilezOptions struct {
		// Name is the profile name to collect (e.g. "cpu", "heap", "goroutine", "allocs").
		Name string `json:"name"`
		// Debug is the debug level for the profile.
		Debug int `json:"debug"`
		// Duration is the CPU profile duration (only used when Name is "cpu"). Must be > 0 and <= 15s.
		Duration time.Duration `json:"duration,omitempty"`
	}

	// ProfilezEventOptions are the options passed to Profilez requests issued over the system events subject.
	ProfilezEventOptions struct {
		ProfilezOptions
		EventFilterOptions
	}
)

// Profilez returns runtime profiling data from a specific server.
// CPU profiles cause the server to sleep on the request path for the configured Duration,
// delaying other system-account monitoring traffic on that server until the profile completes.
// Duration must be > 0 and <= 15s.
func (s *System) Profilez(ctx context.Context, id string, opts ProfilezEventOptions) (*ProfilezResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvProfilezSubj, payload)
	if err != nil {
		return nil, err
	}

	var profilezResp ProfilezResp
	if err := json.Unmarshal(resp.Data, &profilezResp); err != nil {
		return nil, err
	}

	return &profilezResp, nil
}

// ProfilezPing returns runtime profiling data from all servers.
// CPU profiles cause each server to sleep on the request path for the configured Duration
// (fanned out across the cluster, this can meaningfully delay monitoring traffic).
func (s *System) ProfilezPing(ctx context.Context, opts ProfilezEventOptions) ([]ProfilezResp, error) {
	subj := fmt.Sprintf(srvProfilezSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvProfilez := make([]ProfilezResp, 0, len(resp))
	for _, msg := range resp {
		var profilezResp ProfilezResp
		if err := json.Unmarshal(msg.Data, &profilezResp); err != nil {
			return nil, err
		}
		srvProfilez = append(srvProfilez, profilezResp)
	}
	return srvProfilez, nil
}
