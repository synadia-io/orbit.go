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
	// GatewayzResp is the response from a Gatewayz request.
	GatewayzResp struct {
		Server   ServerInfo `json:"server"`
		Gatewayz Gatewayz   `json:"data"`
		Error    APIError   `json:"error,omitempty"`
	}

	// Gatewayz represents detailed information on Gateways.
	Gatewayz struct {
		ID               string                       `json:"server_id"`
		Now              time.Time                    `json:"now"`
		Name             string                       `json:"name,omitempty"`
		Host             string                       `json:"host,omitempty"`
		Port             int                          `json:"port,omitempty"`
		OutboundGateways map[string]*RemoteGatewayz   `json:"outbound_gateways"`
		InboundGateways  map[string][]*RemoteGatewayz `json:"inbound_gateways"`
	}

	// RemoteGatewayz represents information about an outbound connection to a gateway.
	RemoteGatewayz struct {
		IsConfigured bool               `json:"configured"`
		Connection   *ConnInfo          `json:"connection,omitempty"`
		Accounts     []*AccountGatewayz `json:"accounts,omitempty"`
	}

	// AccountGatewayz represents interest mode for an account on a gateway.
	AccountGatewayz struct {
		Name                  string      `json:"name"`
		InterestMode          string      `json:"interest_mode"`
		NoInterestCount       int         `json:"no_interest_count,omitempty"`
		InterestOnlyThreshold int         `json:"interest_only_threshold,omitempty"`
		TotalSubscriptions    int         `json:"num_subs,omitempty"`
		NumQueueSubscriptions int         `json:"num_queue_subs,omitempty"`
		Subs                  []string    `json:"subscriptions_list,omitempty"`
		SubsDetail            []SubDetail `json:"subscriptions_list_detail,omitempty"`
	}

	// GatewayzOptions are the options passed to Gatewayz.
	GatewayzOptions struct {
		// Name filters the list to a single named remote gateway.
		Name string `json:"name"`

		// Accounts indicates if accounts with their interest should be included in the results.
		Accounts bool `json:"accounts"`

		// AccountName limits the list of accounts to a single account (implies Accounts).
		AccountName string `json:"account_name"`

		// AccountSubscriptions indicates if subscriptions should be included in the results.
		// Only used if Accounts or AccountName are specified.
		AccountSubscriptions bool `json:"subscriptions"`

		// AccountSubscriptionsDetail indicates if subscription details should be included in the results.
		// Only used if Accounts or AccountName are specified.
		AccountSubscriptionsDetail bool `json:"subscriptions_detail"`
	}

	// GatewayzEventOptions are the options passed to Gatewayz requests issued over the system events subject.
	GatewayzEventOptions struct {
		GatewayzOptions
		EventFilterOptions
	}
)

// Gatewayz returns gateway connection details from a specific server.
func (s *System) Gatewayz(ctx context.Context, id string, opts GatewayzEventOptions) (*GatewayzResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvGatewayzSubj, payload)
	if err != nil {
		return nil, err
	}

	var gatewayzResp GatewayzResp
	if err := json.Unmarshal(resp.Data, &gatewayzResp); err != nil {
		return nil, err
	}

	return &gatewayzResp, nil
}

// GatewayzPing returns gateway connection details from all servers.
func (s *System) GatewayzPing(ctx context.Context, opts GatewayzEventOptions) ([]GatewayzResp, error) {
	subj := fmt.Sprintf(srvGatewayzSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvGatewayz := make([]GatewayzResp, 0, len(resp))
	for _, msg := range resp {
		var gatewayzResp GatewayzResp
		if err := json.Unmarshal(msg.Data, &gatewayzResp); err != nil {
			return nil, err
		}
		srvGatewayz = append(srvGatewayz, gatewayzResp)
	}
	return srvGatewayz, nil
}
