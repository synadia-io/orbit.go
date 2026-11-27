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
	// LeafzResp is the response from a Leafz request.
	LeafzResp struct {
		Server ServerInfo `json:"server"`
		Leafz  Leafz      `json:"data"`
		Error  APIError   `json:"error,omitempty"`
	}

	// Leafz represents detailed information on leaf node connections.
	Leafz struct {
		ID       string      `json:"server_id"`
		Now      time.Time   `json:"now"`
		NumLeafs int         `json:"leafnodes"`
		Leafs    []*LeafInfo `json:"leafs"`
	}

	// LeafInfo has detailed information on a remote leafnode connection.
	LeafInfo struct {
		ID          uint64     `json:"id"`
		Name        string     `json:"name"`
		IsSpoke     bool       `json:"is_spoke"`
		IsIsolated  bool       `json:"is_isolated,omitempty"`
		Account     string     `json:"account"`
		IP          string     `json:"ip"`
		Port        int        `json:"port"`
		RTT         string     `json:"rtt,omitempty"`
		InMsgs      int64      `json:"in_msgs"`
		OutMsgs     int64      `json:"out_msgs"`
		InBytes     int64      `json:"in_bytes"`
		OutBytes    int64      `json:"out_bytes"`
		NumSubs     uint32     `json:"subscriptions"`
		Subs        []string   `json:"subscriptions_list,omitempty"`
		Compression string     `json:"compression,omitempty"`
		Proxy       *ProxyInfo `json:"proxy,omitempty"`
	}

	// ProxyInfo represents information about a proxied connection.
	ProxyInfo struct {
		Key string `json:"key"`
	}

	// LeafzOptions are options passed to Leafz.
	LeafzOptions struct {
		// Subscriptions indicates that Leafz will return a leafnode's subscriptions.
		Subscriptions bool `json:"subscriptions"`
		// Account filters the list to leafnodes serving a specific account.
		Account string `json:"account"`
	}

	// LeafzEventOptions are the options passed to Leafz requests issued over the system events subject.
	LeafzEventOptions struct {
		LeafzOptions
		EventFilterOptions
	}
)

// Leafz returns leafnode connection details from a specific server.
func (s *System) Leafz(ctx context.Context, id string, opts LeafzEventOptions) (*LeafzResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvLeafzSubj, payload)
	if err != nil {
		return nil, err
	}

	var leafzResp LeafzResp
	if err := json.Unmarshal(resp.Data, &leafzResp); err != nil {
		return nil, err
	}

	return &leafzResp, nil
}

// LeafzPing returns leafnode connection details from all servers.
func (s *System) LeafzPing(ctx context.Context, opts LeafzEventOptions) ([]LeafzResp, error) {
	subj := fmt.Sprintf(srvLeafzSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvLeafz := make([]LeafzResp, 0, len(resp))
	for _, msg := range resp {
		var leafzResp LeafzResp
		if err := json.Unmarshal(msg.Data, &leafzResp); err != nil {
			return nil, err
		}
		srvLeafz = append(srvLeafz, leafzResp)
	}
	return srvLeafz, nil
}
