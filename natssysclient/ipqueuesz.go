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
)

type (
	// IpqueueszResp is the response from an Ipqueuesz request.
	IpqueueszResp struct {
		Server    ServerInfo      `json:"server"`
		Ipqueuesz IpqueueszStatus `json:"data"`
		Error     APIError        `json:"error,omitempty"`
	}

	// IpqueueszStatus maps IPQ names to their current state.
	IpqueueszStatus map[string]IpqueueszStatusIPQ

	// IpqueueszStatusIPQ contains the state of a single IPQ (internal producer queue).
	IpqueueszStatusIPQ struct {
		Pending    int `json:"pending"`
		InProgress int `json:"in_progress,omitempty"`
	}

	// IpqueueszOptions are the options passed to Ipqueuesz.
	IpqueueszOptions struct {
		// All indicates that queues with no pending or in-progress items should also be returned.
		All bool `json:"all"`
		// Filter limits the returned queues to those whose name contains the given substring.
		Filter string `json:"filter"`
	}

	// IpqueueszEventOptions are the options passed to Ipqueuesz requests issued over the system events subject.
	IpqueueszEventOptions struct {
		EventFilterOptions
		IpqueueszOptions
	}
)

// Ipqueuesz returns the internal producer queue state from a specific server.
func (s *System) Ipqueuesz(ctx context.Context, id string, opts IpqueueszEventOptions) (*IpqueueszResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvIpqueueszSubj, payload)
	if err != nil {
		return nil, err
	}

	var ipqueueszResp IpqueueszResp
	if err := json.Unmarshal(resp.Data, &ipqueueszResp); err != nil {
		return nil, err
	}

	return &ipqueueszResp, nil
}

// IpqueueszPing returns the internal producer queue state from all servers.
func (s *System) IpqueueszPing(ctx context.Context, opts IpqueueszEventOptions) ([]IpqueueszResp, error) {
	subj := fmt.Sprintf(srvIpqueueszSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvIpqueuesz := make([]IpqueueszResp, 0, len(resp))
	for _, msg := range resp {
		var ipqueueszResp IpqueueszResp
		if err := json.Unmarshal(msg.Data, &ipqueueszResp); err != nil {
			return nil, err
		}
		srvIpqueuesz = append(srvIpqueuesz, ipqueueszResp)
	}
	return srvIpqueuesz, nil
}
