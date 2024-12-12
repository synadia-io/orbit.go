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

	"github.com/nats-io/jwt"
)

type (
	// VarzResp is the response from the server VARZ request.
	VarzResp struct {
		Server ServerInfo `json:"server"`
		Varz   Varz       `json:"data"`
		Error  APIError   `json:"error,omitempty"`
	}

	// VarzResp is a server response from VARZ endpoint, containing general information about the server.
	Varz struct {
		ID                    string                 `json:"server_id"`
		Name                  string                 `json:"server_name"`
		Version               string                 `json:"version"`
		Proto                 int                    `json:"proto"`
		GitCommit             string                 `json:"git_commit,omitempty"`
		GoVersion             string                 `json:"go"`
		Host                  string                 `json:"host"`
		Port                  int                    `json:"port"`
		AuthRequired          bool                   `json:"auth_required,omitempty"`
		TLSRequired           bool                   `json:"tls_required,omitempty"`
		TLSVerify             bool                   `json:"tls_verify,omitempty"`
		TLSOCSPPeerVerify     bool                   `json:"tls_ocsp_peer_verify,omitempty"`
		IP                    string                 `json:"ip,omitempty"`
		ClientConnectURLs     []string               `json:"connect_urls,omitempty"`
		WSConnectURLs         []string               `json:"ws_connect_urls,omitempty"`
		MaxConn               int                    `json:"max_connections"`
		MaxSubs               int                    `json:"max_subscriptions,omitempty"`
		PingInterval          time.Duration          `json:"ping_interval"`
		MaxPingsOut           int                    `json:"ping_max"`
		HTTPHost              string                 `json:"http_host"`
		HTTPPort              int                    `json:"http_port"`
		HTTPBasePath          string                 `json:"http_base_path"`
		HTTPSPort             int                    `json:"https_port"`
		AuthTimeout           float64                `json:"auth_timeout"`
		MaxControlLine        int32                  `json:"max_control_line"`
		MaxPayload            int                    `json:"max_payload"`
		MaxPending            int64                  `json:"max_pending"`
		Cluster               ClusterOptsVarz        `json:"cluster,omitempty"`
		Gateway               GatewayOptsVarz        `json:"gateway,omitempty"`
		LeafNode              LeafNodeOptsVarz       `json:"leaf,omitempty"`
		MQTT                  MQTTOptsVarz           `json:"mqtt,omitempty"`
		Websocket             WebsocketOptsVarz      `json:"websocket,omitempty"`
		JetStream             JetStreamVarz          `json:"jetstream,omitempty"`
		TLSTimeout            float64                `json:"tls_timeout"`
		WriteDeadline         time.Duration          `json:"write_deadline"`
		Start                 time.Time              `json:"start"`
		Now                   time.Time              `json:"now"`
		Uptime                string                 `json:"uptime"`
		Mem                   int64                  `json:"mem"`
		Cores                 int                    `json:"cores"`
		MaxProcs              int                    `json:"gomaxprocs"`
		CPU                   float64                `json:"cpu"`
		Connections           int                    `json:"connections"`
		TotalConnections      uint64                 `json:"total_connections"`
		Routes                int                    `json:"routes"`
		Remotes               int                    `json:"remotes"`
		Leafs                 int                    `json:"leafnodes"`
		InMsgs                int64                  `json:"in_msgs"`
		OutMsgs               int64                  `json:"out_msgs"`
		InBytes               int64                  `json:"in_bytes"`
		OutBytes              int64                  `json:"out_bytes"`
		SlowConsumers         int64                  `json:"slow_consumers"`
		Subscriptions         uint32                 `json:"subscriptions"`
		HTTPReqStats          map[string]uint64      `json:"http_req_stats"`
		ConfigLoadTime        time.Time              `json:"config_load_time"`
		ConfigDigest          string                 `json:"config_digest"`
		Tags                  jwt.TagList            `json:"tags,omitempty"`
		TrustedOperatorsJwt   []string               `json:"trusted_operators_jwt,omitempty"`
		TrustedOperatorsClaim []*jwt.OperatorClaims  `json:"trusted_operators_claim,omitempty"`
		SystemAccount         string                 `json:"system_account,omitempty"`
		PinnedAccountFail     uint64                 `json:"pinned_account_fails,omitempty"`
		OCSPResponseCache     *OCSPResponseCacheVarz `json:"ocsp_peer_cache,omitempty"`
		SlowConsumersStats    *SlowConsumersStats    `json:"slow_consumer_stats"`
	}

	// ClusterOptsVarz contains monitoring cluster information
	ClusterOptsVarz struct {
		Name        string   `json:"name,omitempty"`
		Host        string   `json:"addr,omitempty"`
		Port        int      `json:"cluster_port,omitempty"`
		AuthTimeout float64  `json:"auth_timeout,omitempty"`
		URLs        []string `json:"urls,omitempty"`
		TLSTimeout  float64  `json:"tls_timeout,omitempty"`
		TLSRequired bool     `json:"tls_required,omitempty"`
		TLSVerify   bool     `json:"tls_verify,omitempty"`
		PoolSize    int      `json:"pool_size,omitempty"`
	}

	// GatewayOptsVarz contains monitoring gateway information
	GatewayOptsVarz struct {
		Name           string                  `json:"name,omitempty"`
		Host           string                  `json:"host,omitempty"`
		Port           int                     `json:"port,omitempty"`
		AuthTimeout    float64                 `json:"auth_timeout,omitempty"`
		TLSTimeout     float64                 `json:"tls_timeout,omitempty"`
		TLSRequired    bool                    `json:"tls_required,omitempty"`
		TLSVerify      bool                    `json:"tls_verify,omitempty"`
		Advertise      string                  `json:"advertise,omitempty"`
		ConnectRetries int                     `json:"connect_retries,omitempty"`
		Gateways       []RemoteGatewayOptsVarz `json:"gateways,omitempty"`
		RejectUnknown  bool                    `json:"reject_unknown,omitempty"` // config got renamed to reject_unknown_cluster
	}

	// RemoteGatewayOptsVarz contains monitoring remote gateway information
	RemoteGatewayOptsVarz struct {
		Name       string   `json:"name"`
		TLSTimeout float64  `json:"tls_timeout,omitempty"`
		URLs       []string `json:"urls,omitempty"`
	}

	// LeafNodeOptsVarz contains monitoring leaf node information
	LeafNodeOptsVarz struct {
		Host              string               `json:"host,omitempty"`
		Port              int                  `json:"port,omitempty"`
		AuthTimeout       float64              `json:"auth_timeout,omitempty"`
		TLSTimeout        float64              `json:"tls_timeout,omitempty"`
		TLSRequired       bool                 `json:"tls_required,omitempty"`
		TLSVerify         bool                 `json:"tls_verify,omitempty"`
		Remotes           []RemoteLeafOptsVarz `json:"remotes,omitempty"`
		TLSOCSPPeerVerify bool                 `json:"tls_ocsp_peer_verify,omitempty"`
	}

	// RemoteLeafOptsVarz contains monitoring remote leaf node information
	RemoteLeafOptsVarz struct {
		LocalAccount      string     `json:"local_account,omitempty"`
		TLSTimeout        float64    `json:"tls_timeout,omitempty"`
		URLs              []string   `json:"urls,omitempty"`
		Deny              *DenyRules `json:"deny,omitempty"`
		TLSOCSPPeerVerify bool       `json:"tls_ocsp_peer_verify,omitempty"`
	}

	// DenyRules Contains lists of subjects not allowed to be imported/exported
	DenyRules struct {
		Exports []string `json:"exports,omitempty"`
		Imports []string `json:"imports,omitempty"`
	}

	// MQTTOptsVarz contains monitoring MQTT information
	MQTTOptsVarz struct {
		Host              string        `json:"host,omitempty"`
		Port              int           `json:"port,omitempty"`
		NoAuthUser        string        `json:"no_auth_user,omitempty"`
		AuthTimeout       float64       `json:"auth_timeout,omitempty"`
		TLSMap            bool          `json:"tls_map,omitempty"`
		TLSTimeout        float64       `json:"tls_timeout,omitempty"`
		TLSPinnedCerts    []string      `json:"tls_pinned_certs,omitempty"`
		JsDomain          string        `json:"js_domain,omitempty"`
		AckWait           time.Duration `json:"ack_wait,omitempty"`
		MaxAckPending     uint16        `json:"max_ack_pending,omitempty"`
		TLSOCSPPeerVerify bool          `json:"tls_ocsp_peer_verify,omitempty"`
	}

	// WebsocketOptsVarz contains monitoring websocket information
	WebsocketOptsVarz struct {
		Host              string        `json:"host,omitempty"`
		Port              int           `json:"port,omitempty"`
		Advertise         string        `json:"advertise,omitempty"`
		NoAuthUser        string        `json:"no_auth_user,omitempty"`
		JWTCookie         string        `json:"jwt_cookie,omitempty"`
		HandshakeTimeout  time.Duration `json:"handshake_timeout,omitempty"`
		AuthTimeout       float64       `json:"auth_timeout,omitempty"`
		NoTLS             bool          `json:"no_tls,omitempty"`
		TLSMap            bool          `json:"tls_map,omitempty"`
		TLSPinnedCerts    []string      `json:"tls_pinned_certs,omitempty"`
		SameOrigin        bool          `json:"same_origin,omitempty"`
		AllowedOrigins    []string      `json:"allowed_origins,omitempty"`
		Compression       bool          `json:"compression,omitempty"`
		TLSOCSPPeerVerify bool          `json:"tls_ocsp_peer_verify,omitempty"`
	}

	// JetStreamVarz contains basic runtime information about jetstream
	JetStreamVarz struct {
		Config *JetStreamConfig `json:"config,omitempty"`
		Stats  *JetStreamStats  `json:"stats,omitempty"`
		Meta   *MetaClusterInfo `json:"meta,omitempty"`
		Limits *JSLimitOpts     `json:"limits,omitempty"`
	}

	// Statistics about JetStream for this server.
	JetStreamStats struct {
		Memory         uint64            `json:"memory"`
		Store          uint64            `json:"storage"`
		ReservedMemory uint64            `json:"reserved_memory"`
		ReservedStore  uint64            `json:"reserved_storage"`
		Accounts       int               `json:"accounts"`
		HAAssets       int               `json:"ha_assets"`
		API            JetStreamAPIStats `json:"api"`
	}

	// JetStreamConfig determines this server's configuration.
	// MaxMemory and MaxStore are in bytes.
	JetStreamConfig struct {
		MaxMemory    int64         `json:"max_memory"`
		MaxStore     int64         `json:"max_storage"`
		StoreDir     string        `json:"store_dir,omitempty"`
		SyncInterval time.Duration `json:"sync_interval,omitempty"`
		SyncAlways   bool          `json:"sync_always,omitempty"`
		Domain       string        `json:"domain,omitempty"`
		CompressOK   bool          `json:"compress_ok,omitempty"`
		UniqueTag    string        `json:"unique_tag,omitempty"`
		Strict       bool          `json:"strict,omitempty"`
	}

	JetStreamAPIStats struct {
		Level    int    `json:"level"`
		Total    uint64 `json:"total"`
		Errors   uint64 `json:"errors"`
		Inflight uint64 `json:"inflight,omitempty"`
	}

	// MetaClusterInfo shows information about the meta group.
	MetaClusterInfo struct {
		Name     string      `json:"name,omitempty"`
		Leader   string      `json:"leader,omitempty"`
		Peer     string      `json:"peer,omitempty"`
		Replicas []*PeerInfo `json:"replicas,omitempty"`
		Size     int         `json:"cluster_size"`
		Pending  int         `json:"pending"`
	}

	// PeerInfo shows information about all the peers in the cluster that
	// are supporting the stream or consumer.
	PeerInfo struct {
		Name    string        `json:"name"`
		Current bool          `json:"current"`
		Offline bool          `json:"offline,omitempty"`
		Active  time.Duration `json:"active"`
		Lag     uint64        `json:"lag,omitempty"`
		Peer    string        `json:"peer"`
	}

	OCSPResponseCacheVarz struct {
		Type      string `json:"cache_type,omitempty"`
		Hits      int64  `json:"cache_hits,omitempty"`
		Misses    int64  `json:"cache_misses,omitempty"`
		Responses int64  `json:"cached_responses,omitempty"`
		Revokes   int64  `json:"cached_revoked_responses,omitempty"`
		Goods     int64  `json:"cached_good_responses,omitempty"`
		Unknowns  int64  `json:"cached_unknown_responses,omitempty"`
	}

	// In the context of system events, VarzEventOptions are options passed to Varz
	VarzEventOptions struct {
		EventFilterOptions
	}

	// Common filter options for system requests STATSZ VARZ SUBSZ CONNZ ROUTEZ GATEWAYZ LEAFZ
	EventFilterOptions struct {
		Name    string   `json:"server_name,omitempty"` // filter by server name
		Cluster string   `json:"cluster,omitempty"`     // filter by cluster name
		Host    string   `json:"host,omitempty"`        // filter by host name
		Tags    []string `json:"tags,omitempty"`        // filter by tags (must match all tags)
		Domain  string   `json:"domain,omitempty"`      // filter by JS domain
	}
)

// Varz returns general server information.
func (s *System) Varz(ctx context.Context, id string, opts VarzEventOptions) (*VarzResp, error) {
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.requestByID(ctx, id, srvVarzSubj, payload)
	if err != nil {
		return nil, err
	}

	var varzResp VarzResp
	if err := json.Unmarshal(resp.Data, &varzResp); err != nil {
		return nil, err
	}

	return &varzResp, nil
}

// VarzPing returns general server information from all servers.
func (s *System) VarzPing(ctx context.Context, opts VarzEventOptions) ([]VarzResp, error) {
	subj := fmt.Sprintf(srvVarzSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.pingServers(ctx, subj, payload)
	if err != nil {
		return nil, err
	}
	srvVarz := make([]VarzResp, 0, len(resp))
	for _, msg := range resp {
		var varzResp VarzResp
		if err := json.Unmarshal(msg.Data, &varzResp); err != nil {
			return nil, err
		}
		srvVarz = append(srvVarz, varzResp)
	}
	return srvVarz, nil
}
