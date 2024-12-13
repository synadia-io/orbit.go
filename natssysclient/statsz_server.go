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

package natssysclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type (
	// ServerStatszResp is the response from the server for the Statsz request.
	ServerStatszResp struct {
		Server ServerInfo  `json:"server"`
		Statsz ServerStats `json:"statsz"`
		Error  APIError    `json:"error,omitempty"`
	}

	// ServerStats hold various statistics that we will periodically send out.
	ServerStats struct {
		Start            time.Time      `json:"start"`
		Mem              int64          `json:"mem"`
		Cores            int            `json:"cores"`
		CPU              float64        `json:"cpu"`
		Connections      int            `json:"connections"`
		TotalConnections uint64         `json:"total_connections"`
		ActiveAccounts   int            `json:"active_accounts"`
		NumSubs          uint32         `json:"subscriptions"`
		Sent             DataStats      `json:"sent"`
		Received         DataStats      `json:"received"`
		SlowConsumers    int64          `json:"slow_consumers"`
		Routes           []*RouteStat   `json:"routes,omitempty"`
		Gateways         []*GatewayStat `json:"gateways,omitempty"`
		ActiveServers    int            `json:"active_servers,omitempty"`
		JetStream        *JetStreamVarz `json:"jetstream,omitempty"`
	}

	// DataStats reports how may msg and bytes. Applicable for both sent and received.
	DataStats struct {
		Msgs  int64 `json:"msgs"`
		Bytes int64 `json:"bytes"`
	}

	// RouteStat holds route statistics.
	RouteStat struct {
		ID       uint64    `json:"rid"`
		Name     string    `json:"name,omitempty"`
		Sent     DataStats `json:"sent"`
		Received DataStats `json:"received"`
		Pending  int       `json:"pending"`
	}

	// GatewayStat holds gateway statistics.
	GatewayStat struct {
		ID         uint64    `json:"gwid"`
		Name       string    `json:"name"`
		Sent       DataStats `json:"sent"`
		Received   DataStats `json:"received"`
		NumInbound int       `json:"inbound_connections"`
	}

	// SlowConsumersStats reports slow consumers stats.
	SlowConsumersStats struct {
		Clients  uint64 `json:"clients"`
		Routes   uint64 `json:"routes"`
		Gateways uint64 `json:"gateways"`
		Leafs    uint64 `json:"leafs"`
	}

	// StatszEventOptions are options passed to Statsz
	StatszEventOptions struct {
		EventFilterOptions
	}
)

// ServerStatsz returns server statistics for the Statsz structs.
func (s *System) ServerStatsz(ctx context.Context, id string, opts StatszEventOptions) (*ServerStatszResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvStatszSubj, payload)
	if err != nil {
		return nil, err
	}

	var statszResp ServerStatszResp
	if err := json.Unmarshal(resp.Data, &statszResp); err != nil {
		return nil, err
	}

	return &statszResp, nil
}

// ServerStatszPing returns server statistics for the Statsz structs from all servers.
func (s *System) ServerStatszPing(ctx context.Context, opts StatszEventOptions) ([]ServerStatszResp, error) {
	subj := fmt.Sprintf(srvStatszSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvStatsz := make([]ServerStatszResp, 0, len(resp))
	for _, msg := range resp {
		var statszResp ServerStatszResp
		if err := json.Unmarshal(msg.Data, &statszResp); err != nil {
			return nil, err
		}
		srvStatsz = append(srvStatsz, statszResp)
	}
	return srvStatsz, nil
}
