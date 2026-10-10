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

	"github.com/nats-io/nats.go"
)

type (
	// RaftzResp is the response from a Raftz request.
	RaftzResp struct {
		Server ServerInfo  `json:"server"`
		Raftz  RaftzStatus `json:"data"`
		Error  APIError    `json:"error,omitempty"`
	}

	// RaftzStatus maps account names to their Raft groups.
	RaftzStatus map[string]map[string]RaftzGroup

	// RaftzGroup contains state for a single Raft group.
	RaftzGroup struct {
		ID            string                    `json:"id"`
		State         string                    `json:"state"`
		Size          int                       `json:"size"`
		QuorumNeeded  int                       `json:"quorum_needed"`
		Observer      bool                      `json:"observer,omitempty"`
		Paused        bool                      `json:"paused,omitempty"`
		Committed     uint64                    `json:"committed"`
		Applied       uint64                    `json:"applied"`
		CatchingUp    bool                      `json:"catching_up,omitempty"`
		Leader        string                    `json:"leader,omitempty"`
		LeaderSince   *time.Time                `json:"leader_since,omitempty"`
		EverHadLeader bool                      `json:"ever_had_leader"`
		Term          uint64                    `json:"term"`
		Vote          string                    `json:"voted_for,omitempty"`
		PTerm         uint64                    `json:"pterm"`
		PIndex        uint64                    `json:"pindex"`
		SystemAcc     bool                      `json:"system_account"`
		TrafficAcc    string                    `json:"traffic_account"`
		IPQPropLen    int                       `json:"ipq_proposal_len"`
		IPQEntryLen   int                       `json:"ipq_entry_len"`
		IPQRespLen    int                       `json:"ipq_resp_len"`
		IPQApplyLen   int                       `json:"ipq_apply_len"`
		WAL           nats.StreamState          `json:"wal"`
		WALError      string                    `json:"wal_error,omitempty"`
		Peers         map[string]RaftzGroupPeer `json:"peers"`
	}

	// RaftzGroupPeer contains state for a single peer within a Raft group.
	RaftzGroupPeer struct {
		Name                string `json:"name"`
		Known               bool   `json:"known"`
		LastReplicatedIndex uint64 `json:"last_replicated_index,omitempty"`
		LastSeen            string `json:"last_seen,omitempty"`
	}

	// RaftzOptions are the options passed to Raftz.
	RaftzOptions struct {
		// AccountFilter limits the results to a single account. Defaults to the system account.
		AccountFilter string `json:"account"`
		// GroupFilter limits the results to a single Raft group.
		GroupFilter string `json:"group"`
	}

	// RaftzEventOptions are the options passed to Raftz requests issued over the system events subject.
	RaftzEventOptions struct {
		EventFilterOptions
		RaftzOptions
	}
)

// Raftz returns Raft group state from a specific server.
func (s *System) Raftz(ctx context.Context, id string, opts RaftzEventOptions) (*RaftzResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvRaftzSubj, payload)
	if err != nil {
		return nil, err
	}

	var raftzResp RaftzResp
	if err := json.Unmarshal(resp.Data, &raftzResp); err != nil {
		return nil, err
	}

	return &raftzResp, nil
}

// RaftzPing returns Raft group state from all servers.
func (s *System) RaftzPing(ctx context.Context, opts RaftzEventOptions) ([]RaftzResp, error) {
	subj := fmt.Sprintf(srvRaftzSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvRaftz := make([]RaftzResp, 0, len(resp))
	for _, msg := range resp {
		var raftzResp RaftzResp
		if err := json.Unmarshal(msg.Data, &raftzResp); err != nil {
			return nil, err
		}
		srvRaftz = append(srvRaftz, raftzResp)
	}
	return srvRaftz, nil
}
