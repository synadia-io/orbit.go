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
	// ExpvarzResp is the response from an Expvarz request.
	ExpvarzResp struct {
		Server  ServerInfo    `json:"server"`
		Expvarz ExpvarzStatus `json:"data"`
		Error   APIError      `json:"error,omitempty"`
	}

	// ExpvarzStatus contains the runtime variables exported via Go's expvar package.
	ExpvarzStatus struct {
		Memstats json.RawMessage `json:"memstats"`
		Cmdline  json.RawMessage `json:"cmdline"`
	}

	// ExpvarzEventOptions are the options passed to Expvarz requests issued over the system events subject.
	ExpvarzEventOptions struct {
		EventFilterOptions
	}
)

// Expvarz returns runtime variables from a specific server.
func (s *System) Expvarz(ctx context.Context, id string, opts ExpvarzEventOptions) (*ExpvarzResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvExpvarzSubj, payload)
	if err != nil {
		return nil, err
	}

	var expvarzResp ExpvarzResp
	if err := json.Unmarshal(resp.Data, &expvarzResp); err != nil {
		return nil, err
	}

	return &expvarzResp, nil
}

// ExpvarzPing returns runtime variables from all servers.
func (s *System) ExpvarzPing(ctx context.Context, opts ExpvarzEventOptions) ([]ExpvarzResp, error) {
	subj := fmt.Sprintf(srvExpvarzSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvExpvarz := make([]ExpvarzResp, 0, len(resp))
	for _, msg := range resp {
		var expvarzResp ExpvarzResp
		if err := json.Unmarshal(msg.Data, &expvarzResp); err != nil {
			return nil, err
		}
		srvExpvarz = append(srvExpvarz, expvarzResp)
	}
	return srvExpvarz, nil
}
