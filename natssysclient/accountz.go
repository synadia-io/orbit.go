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
	"net/http"
	"time"

	"github.com/nats-io/jwt/v2"
)

type (
	// AccountzResp is the response from an Accountz request.
	AccountzResp struct {
		Server   ServerInfo `json:"server"`
		Accountz Accountz   `json:"data"`
		Error    APIError   `json:"error,omitempty"`
	}

	// Accountz contains information about accounts on a server.
	// When requested without a specific account, only Accounts is populated with the list of known account names.
	// When a specific account is requested, Account contains the full account details.
	Accountz struct {
		ID            string       `json:"server_id"`
		Now           time.Time    `json:"now"`
		SystemAccount string       `json:"system_account,omitempty"`
		Accounts      []string     `json:"accounts,omitempty"`
		Account       *AccountInfo `json:"account_detail,omitempty"`
	}

	// AccountInfo contains detailed information about a single account.
	AccountInfo struct {
		AccountName string               `json:"account_name"`
		LastUpdate  time.Time            `json:"update_time,omitempty"`
		IsSystem    bool                 `json:"is_system,omitempty"`
		Expired     bool                 `json:"expired"`
		Complete    bool                 `json:"complete"`
		JetStream   bool                 `json:"jetstream_enabled"`
		LeafCnt     int                  `json:"leafnode_connections"`
		ClientCnt   int                  `json:"client_connections"`
		SubCnt      uint32               `json:"subscriptions"`
		Mappings    ExtMap               `json:"mappings,omitempty"`
		Exports     []ExtExport          `json:"exports,omitempty"`
		Imports     []ExtImport          `json:"imports,omitempty"`
		Jwt         string               `json:"jwt,omitempty"`
		IssuerKey   string               `json:"issuer_key,omitempty"`
		NameTag     string               `json:"name_tag,omitempty"`
		Tags        jwt.TagList          `json:"tags,omitempty"`
		Claim       *jwt.AccountClaims   `json:"decoded_jwt,omitempty"`
		Vr          []ExtVrIssues        `json:"validation_result_jwt,omitempty"`
		RevokedUser map[string]time.Time `json:"revoked_user,omitempty"`
		Sublist     *SublistStats        `json:"sublist_stats,omitempty"`
		Responses   map[string]ExtImport `json:"responses,omitempty"`
	}

	// ExtMap is the map of subject mappings on an account.
	ExtMap map[string][]*MapDest

	// MapDest describes a single weighted destination for a subject mapping.
	MapDest struct {
		Subject string `json:"subject"`
		Weight  uint8  `json:"weight"`
		Cluster string `json:"cluster,omitempty"`
	}

	// ExtExport is a wrapper around jwt.Export adding account-scoped metadata.
	ExtExport struct {
		jwt.Export
		ApprovedAccounts []string             `json:"approved_accounts,omitempty"`
		RevokedAct       map[string]time.Time `json:"revoked_activations,omitempty"`
	}

	// ExtImport is a wrapper around jwt.Import adding account-scoped metadata
	// and latency-tracking state. M1 carries the last-observed ServiceLatency
	// measurement as raw JSON: its concrete server-side type transitively depends
	// on nats-server internals (TypedEvent, ClientInfo) that are not exported by
	// this module; decode it yourself against the shape published by nats-server
	// if you need the fields.
	ExtImport struct {
		jwt.Import
		Invalid     bool                `json:"invalid"`
		Share       bool                `json:"share"`
		Tracking    bool                `json:"tracking"`
		TrackingHdr http.Header         `json:"tracking_header,omitempty"`
		Latency     *jwt.ServiceLatency `json:"latency,omitempty"`
		M1          json.RawMessage     `json:"m1,omitempty"`
	}

	// ExtVrIssues is a single JWT claim validation issue.
	ExtVrIssues struct {
		Description string `json:"description"`
		Blocking    bool   `json:"blocking"`
		Time        bool   `json:"time_check"`
	}

	// AccountzOptions are the options passed to Accountz.
	AccountzOptions struct {
		// Account is the name of the account to request details for.
		// When empty, the response only contains the list of account names.
		Account string `json:"account"`
	}

	// AccountzEventOptions are the options passed to Accountz requests issued over the system events subject.
	AccountzEventOptions struct {
		AccountzOptions
		EventFilterOptions
	}
)

// Accountz returns account information from a specific server.
func (s *System) Accountz(ctx context.Context, id string, opts AccountzEventOptions) (*AccountzResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvAccountzSubj, payload)
	if err != nil {
		return nil, err
	}

	var accountzResp AccountzResp
	if err := json.Unmarshal(resp.Data, &accountzResp); err != nil {
		return nil, err
	}

	return &accountzResp, nil
}

// AccountzPing returns account information from all servers.
func (s *System) AccountzPing(ctx context.Context, opts AccountzEventOptions) ([]AccountzResp, error) {
	subj := fmt.Sprintf(srvAccountzSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvAccountz := make([]AccountzResp, 0, len(resp))
	for _, msg := range resp {
		var accountzResp AccountzResp
		if err := json.Unmarshal(msg.Data, &accountzResp); err != nil {
			return nil, err
		}
		srvAccountz = append(srvAccountz, accountzResp)
	}
	return srvAccountz, nil
}
