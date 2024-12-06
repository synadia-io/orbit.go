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
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	// JSZResp is the response from the JSZ request.
	JSZResp struct {
		Server ServerInfo `json:"server"`
		JSInfo JSInfo     `json:"data"`
		Error  APIError   `json:"error,omitempty"`
	}

	// JSInfo is the JetStream information for the server.
	JSInfo struct {
		ID       string          `json:"server_id"`
		Now      time.Time       `json:"now"`
		Disabled bool            `json:"disabled,omitempty"`
		Config   JetStreamConfig `json:"config,omitempty"`
		Limits   *JSLimitOpts    `json:"limits,omitempty"`
		JetStreamStats
		Streams   int              `json:"streams"`
		Consumers int              `json:"consumers"`
		Messages  uint64           `json:"messages"`
		Bytes     uint64           `json:"bytes"`
		Meta      *MetaClusterInfo `json:"meta_cluster,omitempty"`

		// aggregate raft info
		AccountDetails []*AccountDetail `json:"account_details,omitempty"`
	}

	// AccountDetail contains the details for an account.
	AccountDetail struct {
		Name string `json:"name"`
		Id   string `json:"id"`
		JetStreamStats
		Streams []StreamDetail `json:"stream_detail,omitempty"`
	}

	// StreamDetail contains the details for a stream.
	StreamDetail struct {
		Name               string                   `json:"name"`
		Created            time.Time                `json:"created"`
		Cluster            *nats.ClusterInfo        `json:"cluster,omitempty"`
		Config             *nats.StreamConfig       `json:"config,omitempty"`
		State              nats.StreamState         `json:"state,omitempty"`
		Consumer           []*nats.ConsumerInfo     `json:"consumer_detail,omitempty"`
		Mirror             *nats.StreamSourceInfo   `json:"mirror,omitempty"`
		Sources            []*nats.StreamSourceInfo `json:"sources,omitempty"`
		RaftGroup          string                   `json:"stream_raft_group,omitempty"`
		ConsumerRaftGroups []*RaftGroupDetail       `json:"consumer_raft_groups,omitempty"`
	}

	// RaftGroupDetail contains the details for a raft group.
	RaftGroupDetail struct {
		Name      string `json:"name"`
		RaftGroup string `json:"raft_group,omitempty"`
	}

	// JSLimitOpts are the JetStream limits for the server.
	JSLimitOpts struct {
		MaxRequestBatch int           `json:"max_request_batch,omitempty"`
		MaxAckPending   int           `json:"max_ack_pending,omitempty"`
		MaxHAAssets     int           `json:"max_ha_assets,omitempty"`
		Duplicates      time.Duration `json:"max_duplicate_window,omitempty"`
	}

	// JsEventOptions are the options for the JSZ request.
	JszEventOptions struct {
		JszOptions
		EventFilterOptions
	}

	// JszOptions are the options for the JSZ request.
	JszOptions struct {
		// Account to filter on.
		Account string `json:"account,omitempty"`

		// Include account specific JetStream information
		Accounts bool `json:"accounts,omitempty"`

		// Include streams. When set, implies Accounts is set.
		Streams bool `json:"streams,omitempty"`

		// Include consumers. When set, implies Streams is set.
		Consumer bool `json:"consumer,omitempty"`

		// When stream or consumer are requested, include their respective configuration.
		Config bool `json:"config,omitempty"`

		// Only the leader responds.
		LeaderOnly bool `json:"leader_only,omitempty"`

		// Pagination offset.
		Offset int `json:"offset,omitempty"`

		// Pagination limit.
		Limit int `json:"limit,omitempty"`

		// Include detailed information about the raft group.
		RaftGroups bool `json:"raft,omitempty"`

		// Returns stream details only for the stream in which the server is the leader for that stream.
		StreamLeaderOnly bool `json:"stream_leader_only,omitempty"`
	}
)

// Jsz returns server jetstream details.
func (s *System) Jsz(ctx context.Context, id string, opts JszEventOptions) (*JSZResp, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: server id cannot be empty", ErrValidation)
	}
	conn := s.nc
	subj := fmt.Sprintf(srvJszSubj, id)
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultRequestTimeout)
		defer cancel()
	}
	resp, err := conn.RequestWithContext(ctx, subj, payload)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidServerID, id)
		}
		return nil, err
	}

	var jszResp JSZResp
	if err := json.Unmarshal(resp.Data, &jszResp); err != nil {
		return nil, err
	}

	return &jszResp, nil
}

// JszPing returns server jetstream details for all servers.
func (s *System) JszPing(ctx context.Context, opts JszEventOptions) ([]JSZResp, error) {
	subj := fmt.Sprintf(srvJszSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvJsz := make([]JSZResp, 0, len(resp))
	for _, msg := range resp {
		var jszResp JSZResp
		if err := json.Unmarshal(msg.Data, &jszResp); err != nil {
			return nil, err
		}
		srvJsz = append(srvJsz, jszResp)
	}
	return srvJsz, nil
}
