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
	"iter"
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
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvJszSubj, payload)
	if err != nil {
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

// AllJsz returns an iterator over JSZ for a specific server,
// automatically handling pagination when AccountDetails are requested.
// Note: Pagination only applies when opts.Accounts is true.
func (s *System) AllJsz(ctx context.Context, id string, opts JszEventOptions) iter.Seq2[*JSZResp, error] {
	return func(yield func(*JSZResp, error) bool) {
		// If Accounts is not requested, pagination doesn't apply
		if !opts.Accounts {
			resp, err := s.Jsz(ctx, id, opts)
			if err != nil {
				yield(nil, err)
				return
			}
			yield(resp, nil)
			return
		}

		currentOpts := opts

		for {
			resp, err := s.Jsz(ctx, id, currentOpts)
			if err != nil {
				yield(nil, err)
				return
			}

			if !yield(resp, nil) {
				return
			}

			// Check if we've received all accounts
			// The total is determined by the Accounts field in JetStreamStats
			received := currentOpts.Offset + len(resp.JSInfo.AccountDetails)
			if received >= resp.JSInfo.Accounts || len(resp.JSInfo.AccountDetails) == 0 {
				return
			}

			// Update offset for next page
			currentOpts.Offset = received
		}
	}
}

// AllJszPing returns a slice of iterators, one for each server,
// automatically handling pagination for each server independently when AccountDetails are requested.
func (s *System) AllJszPing(ctx context.Context, opts JszEventOptions) ([]iter.Seq2[*JSZResp, error], error) {
	// First get initial responses from all servers
	initialResponses, err := s.JszPing(ctx, opts)
	if err != nil {
		return nil, err
	}

	// Create an iterator for each server
	iterators := make([]iter.Seq2[*JSZResp, error], len(initialResponses))

	for i, initial := range initialResponses {
		// Capture variables for closure
		serverID := initial.Server.ID
		firstResp := initial
		needsPagination := opts.Accounts && firstResp.JSInfo.Accounts > 0

		iterators[i] = func(yield func(*JSZResp, error) bool) {
			// Yield the initial response
			resp := firstResp
			if !yield(&resp, nil) {
				return
			}

			// If Accounts is not requested, no pagination needed
			if !needsPagination {
				return
			}

			// Continue pagination for this server if needed
			currentOpts := opts
			currentOpts.Offset = opts.Offset + len(firstResp.JSInfo.AccountDetails)

			for currentOpts.Offset < firstResp.JSInfo.Accounts {
				nextResp, err := s.Jsz(ctx, serverID, currentOpts)
				if err != nil {
					yield(nil, err)
					return
				}

				if !yield(nextResp, nil) {
					return
				}

				currentOpts.Offset += len(nextResp.JSInfo.AccountDetails)
			}
		}
	}

	return iterators, nil
}
