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

// ServerID is the response from the IDZ request. Unlike other endpoints,
// IDZ returns the server identification info directly without a response envelope.
type ServerID struct {
	Name string `json:"name"`
	Host string `json:"host"`
	ID   string `json:"id"`
}

// Idz returns basic identification information of a specific server.
func (s *System) Idz(ctx context.Context, id string) (*ServerID, error) {
	resp, err := s.requestByID(ctx, id, srvIdzSubj, nil)
	if err != nil {
		return nil, err
	}

	var idzResp ServerID
	if err := json.Unmarshal(resp.Data, &idzResp); err != nil {
		return nil, err
	}

	return &idzResp, nil
}

// IdzPing returns basic identification information from all servers.
func (s *System) IdzPing(ctx context.Context) ([]ServerID, error) {
	subj := fmt.Sprintf(srvIdzSubj, "PING")
	resp, err := s.pingServers(ctx, subj, nil)
	if err != nil {
		return nil, err
	}
	srvIdz := make([]ServerID, 0, len(resp))
	for _, msg := range resp {
		var idzResp ServerID
		if err := json.Unmarshal(msg.Data, &idzResp); err != nil {
			return nil, err
		}
		srvIdz = append(srvIdz, idzResp)
	}
	return srvIdz, nil
}
