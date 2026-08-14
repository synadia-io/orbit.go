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
	// RoutezResp is the response from a Routez request.
	RoutezResp struct {
		Server ServerInfo `json:"server"`
		Routez Routez     `json:"data"`
		Error  APIError   `json:"error,omitempty"`
	}

	// Routez contains detailed information on current route connections.
	Routez struct {
		ID        string             `json:"server_id"`
		Name      string             `json:"server_name"`
		Now       time.Time          `json:"now"`
		Import    *SubjectPermission `json:"import,omitempty"`
		Export    *SubjectPermission `json:"export,omitempty"`
		NumRoutes int                `json:"num_routes"`
		Routes    []*RouteInfo       `json:"routes"`
	}

	// SubjectPermission represents allow/deny lists for subjects.
	SubjectPermission struct {
		Allow []string `json:"allow,omitempty"`
		Deny  []string `json:"deny,omitempty"`
	}

	// RouteInfo has detailed information on a per connection basis.
	RouteInfo struct {
		Rid          uint64             `json:"rid"`
		RemoteID     string             `json:"remote_id"`
		RemoteName   string             `json:"remote_name"`
		DidSolicit   bool               `json:"did_solicit"`
		IsConfigured bool               `json:"is_configured"`
		IP           string             `json:"ip"`
		Port         int                `json:"port"`
		Start        time.Time          `json:"start"`
		LastActivity time.Time          `json:"last_activity"`
		RTT          string             `json:"rtt,omitempty"`
		Uptime       string             `json:"uptime"`
		Idle         string             `json:"idle"`
		Import       *SubjectPermission `json:"import,omitempty"`
		Export       *SubjectPermission `json:"export,omitempty"`
		Pending      int                `json:"pending_size"`
		InMsgs       int64              `json:"in_msgs"`
		OutMsgs      int64              `json:"out_msgs"`
		InBytes      int64              `json:"in_bytes"`
		OutBytes     int64              `json:"out_bytes"`
		NumSubs      uint32             `json:"subscriptions"`
		Subs         []string           `json:"subscriptions_list,omitempty"`
		SubsDetail   []SubDetail        `json:"subscriptions_list_detail,omitempty"`
		Account      string             `json:"account,omitempty"`
		Compression  string             `json:"compression,omitempty"`
	}

	// RoutezOptions are options passed to Routez.
	RoutezOptions struct {
		// Subscriptions indicates that Routez will return a route's subscriptions.
		Subscriptions bool `json:"subscriptions"`
		// SubscriptionsDetail indicates if subscription details should be included in the results.
		SubscriptionsDetail bool `json:"subscriptions_detail"`
	}

	// RoutezEventOptions are the options passed to Routez requests issued over the system events subject.
	RoutezEventOptions struct {
		RoutezOptions
		EventFilterOptions
	}
)

// Routez returns route connection details from a specific server.
func (s *System) Routez(ctx context.Context, id string, opts RoutezEventOptions) (*RoutezResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvRoutezSubj, payload)
	if err != nil {
		return nil, err
	}

	var routezResp RoutezResp
	if err := json.Unmarshal(resp.Data, &routezResp); err != nil {
		return nil, err
	}

	return &routezResp, nil
}

// RoutezPing returns route connection details from all servers.
func (s *System) RoutezPing(ctx context.Context, opts RoutezEventOptions) ([]RoutezResp, error) {
	subj := fmt.Sprintf(srvRoutezSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvRoutez := make([]RoutezResp, 0, len(resp))
	for _, msg := range resp {
		var routezResp RoutezResp
		if err := json.Unmarshal(msg.Data, &routezResp); err != nil {
			return nil, err
		}
		srvRoutez = append(srvRoutez, routezResp)
	}
	return srvRoutez, nil
}
