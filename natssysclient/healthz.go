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
)

type (
	// HealthzResp is the response from the Healthz request.
	HealthzResp struct {
		Server  ServerInfo `json:"server"`
		Healthz Healthz    `json:"data"`
		Error   APIError   `json:"error,omitempty"`
	}

	// Healthz is the health status of the server.
	Healthz struct {
		Status     string         `json:"status"`
		StatusCode int            `json:"status_code,omitempty"`
		Error      string         `json:"error,omitempty"`
		Errors     []HealthzError `json:"errors,omitempty"`
	}

	// HealthzError is an error returned by the Healthz request.
	HealthzError struct {
		Type     HealthZErrorType `json:"type"`
		Account  string           `json:"account,omitempty"`
		Stream   string           `json:"stream,omitempty"`
		Consumer string           `json:"consumer,omitempty"`
		Error    string           `json:"error,omitempty"`
	}

	// HealthZErrorType is the type of error returned by the Healthz request.
	HealthZErrorType int

	// HealthzOptions are options passed to Healthz.
	HealthzOptions struct {
		// If set to true, only check if the server is connected to JetStream (without checking assets).
		JSEnabledOnly bool `json:"js-enabled-only,omitempty"`

		// If set to true, only check if the server is healthy (without checking JetStream).
		JSServerOnly bool `json:"js-server-only,omitempty"`

		// Used to check the health of a specific account. Must be used when requesting stream or consumer health.
		Account string `json:"account,omitempty"`

		// Used to check the health of a specific stream.
		Stream string `json:"stream,omitempty"`

		// Used to check the health of a specific consumer.
		Consumer string `json:"consumer,omitempty"`

		// If set to true, return detailed information about errors (if any).
		Details bool `json:"details,omitempty"`
	}
)

const (
	HealthzErrorConn HealthZErrorType = iota
	HealthzErrorBadRequest
	HealthzErrorJetStream
	HealthzErrorAccount
	HealthzErrorStream
	HealthzErrorConsumer
)

// Healthz checks server health status.
func (s *System) Healthz(ctx context.Context, id string, opts HealthzOptions) (*HealthzResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvHealthzSubj, payload)
	if err != nil {
		return nil, err
	}

	var healthzResp HealthzResp
	if err := json.Unmarshal(resp.Data, &healthzResp); err != nil {
		return nil, err
	}

	return &healthzResp, nil
}

// HealthzPing checks the health status of all servers.
func (s *System) HealthzPing(ctx context.Context, opts HealthzOptions) ([]HealthzResp, error) {
	subj := fmt.Sprintf(srvHealthzSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvHealthz := make([]HealthzResp, 0, len(resp))
	for _, msg := range resp {
		var healthzResp HealthzResp
		if err := json.Unmarshal(msg.Data, &healthzResp); err != nil {
			return nil, err
		}
		srvHealthz = append(srvHealthz, healthzResp)
	}
	return srvHealthz, nil
}
