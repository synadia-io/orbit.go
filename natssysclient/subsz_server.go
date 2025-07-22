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
)

type (
	// SubszResp is the response from the server subscriptions request.
	SubszResp struct {
		Server ServerInfo `json:"server"`
		Subsz  Subsz      `json:"data"`
		Error  APIError   `json:"error,omitempty"`
	}

	// Subsz is the server subscriptions data.
	Subsz struct {
		ID  string    `json:"server_id"`
		Now time.Time `json:"now"`
		*SublistStats
		Total  int         `json:"total"`
		Offset int         `json:"offset"`
		Limit  int         `json:"limit"`
		Subs   []SubDetail `json:"subscriptions_list,omitempty"`
	}

	// SublistStats are the statistics for the subscriptions list.
	SublistStats struct {
		NumSubs      uint32  `json:"num_subscriptions"`
		NumCache     uint32  `json:"num_cache"`
		NumInserts   uint64  `json:"num_inserts"`
		NumRemoves   uint64  `json:"num_removes"`
		NumMatches   uint64  `json:"num_matches"`
		CacheHitRate float64 `json:"cache_hit_rate"`
		MaxFanout    uint32  `json:"max_fanout"`
		AvgFanout    float64 `json:"avg_fanout"`
	}

	// SubszOptions are the options passed to Subsz.
	SubszOptions struct {
		// Offset is used for pagination. Subsz() only returns connections starting at this
		// offset from the global results.
		Offset int `json:"offset"`

		// Limit is the maximum number of subscriptions that should be returned by Subsz().
		Limit int `json:"limit"`

		// Subscriptions indicates if subscription details should be included in the results.
		Subscriptions bool `json:"subscriptions"`

		// Filter based on this account name.
		Account string `json:"account,omitempty"`

		// Test the list against this subject. Needs to be literal since it signifies a publish subject.
		// We will only return subscriptions that would match if a message was sent to this subject.
		Test string `json:"test,omitempty"`
	}
)

// ServerSubsz returns server subscriptions data
func (s *System) ServerSubsz(ctx context.Context, id string, opts SubszOptions) (*SubszResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvSubszSubj, payload)
	if err != nil {
		return nil, err
	}

	var subszResp SubszResp
	if err := json.Unmarshal(resp.Data, &subszResp); err != nil {
		return nil, err
	}

	return &subszResp, nil
}

// ServerSubszPing returns server subscriptions data from all servers.
func (s *System) ServerSubszPing(ctx context.Context, opts SubszOptions) ([]SubszResp, error) {
	subj := fmt.Sprintf(srvSubszSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvSubsz := make([]SubszResp, 0, len(resp))
	for _, msg := range resp {
		var subszResp SubszResp
		if err := json.Unmarshal(msg.Data, &subszResp); err != nil {
			return nil, err
		}
		srvSubsz = append(srvSubsz, subszResp)
	}
	return srvSubsz, nil
}

// AllServerSubsz returns an iterator over all subscriptions for a specific server,
// automatically handling pagination.
func (s *System) AllServerSubsz(ctx context.Context, id string, opts SubszOptions) iter.Seq2[*SubszResp, error] {
	return func(yield func(*SubszResp, error) bool) {
		// Start with the provided offset
		currentOpts := opts

		for {
			resp, err := s.ServerSubsz(ctx, id, currentOpts)
			if err != nil {
				yield(nil, err)
				return
			}

			if !yield(resp, nil) {
				return
			}

			// Check if we've received all subscriptions
			received := currentOpts.Offset + len(resp.Subsz.Subs)
			if received >= resp.Subsz.Total || len(resp.Subsz.Subs) == 0 {
				return
			}

			// Update offset for next page
			currentOpts.Offset = received
		}
	}
}

// AllServerSubszPing returns a slice of iterators, one for each server,
// automatically handling pagination for each server independently.
func (s *System) AllServerSubszPing(ctx context.Context, opts SubszOptions) ([]iter.Seq2[*SubszResp, error], error) {
	// First get initial responses from all servers
	initialResponses, err := s.ServerSubszPing(ctx, opts)
	if err != nil {
		return nil, err
	}

	// Create an iterator for each server
	iterators := make([]iter.Seq2[*SubszResp, error], len(initialResponses))

	for i, initial := range initialResponses {
		serverID := initial.Server.ID
		firstResp := initial

		iterators[i] = func(yield func(*SubszResp, error) bool) {
			// Yield the initial response
			resp := firstResp
			if !yield(&resp, nil) {
				return
			}

			// Continue pagination for this server if needed
			currentOpts := opts
			currentOpts.Offset = opts.Offset + len(firstResp.Subsz.Subs)

			for currentOpts.Offset < firstResp.Subsz.Total {
				nextResp, err := s.ServerSubsz(ctx, serverID, currentOpts)
				if err != nil {
					yield(nil, err)
					return
				}

				if !yield(nextResp, nil) {
					return
				}

				currentOpts.Offset += len(nextResp.Subsz.Subs)
			}
		}
	}

	return iterators, nil
}
