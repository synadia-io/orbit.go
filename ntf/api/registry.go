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

package api

import (
	"embed"
	"fmt"
	"reflect"
	"strings"
)

//go:embed schemas
var schemaFS embed.FS

// typePrefix is the part of every type id that is not in its schema path.
const typePrefix = "io.synadia."

// The type ids of the requests and responses of the tester endpoints. Each is
// the snake-cased name of its Go type below io.synadia.ntf.tester.v1.
const (
	TypeCreateServerRequest       = "io.synadia.ntf.tester.v1.create_server_request"
	TypeCreateClusterRequest      = "io.synadia.ntf.tester.v1.create_cluster_request"
	TypeCreateSuperClusterRequest = "io.synadia.ntf.tester.v1.create_super_cluster_request"
	TypeCreateResponse            = "io.synadia.ntf.tester.v1.create_response"
	TypeStopServerRequest         = "io.synadia.ntf.tester.v1.stop_server_request"
	TypeStopServerResponse        = "io.synadia.ntf.tester.v1.stop_server_response"
	TypeStopInstanceRequest       = "io.synadia.ntf.tester.v1.stop_instance_request"
	TypeStopInstanceResponse      = "io.synadia.ntf.tester.v1.stop_instance_response"
	TypeStartServerRequest        = "io.synadia.ntf.tester.v1.start_server_request"
	TypeStartServerResponse       = "io.synadia.ntf.tester.v1.start_server_response"
	TypeStartInstanceRequest      = "io.synadia.ntf.tester.v1.start_instance_request"
	TypeStartInstanceResponse     = "io.synadia.ntf.tester.v1.start_instance_response"
	TypeUpdateServerRequest       = "io.synadia.ntf.tester.v1.update_server_request"
	TypeUpdateServerResponse      = "io.synadia.ntf.tester.v1.update_server_response"
	TypeReloadServerRequest       = "io.synadia.ntf.tester.v1.reload_server_request"
	TypeReloadServerResponse      = "io.synadia.ntf.tester.v1.reload_server_response"
	TypeShapeSetRequest           = "io.synadia.ntf.tester.v1.shape_set_request"
	TypeShapeSetResponse          = "io.synadia.ntf.tester.v1.shape_set_response"
	TypeShapeClearRequest         = "io.synadia.ntf.tester.v1.shape_clear_request"
	TypeShapeClearResponse        = "io.synadia.ntf.tester.v1.shape_clear_response"
	TypeShapeReportRequest        = "io.synadia.ntf.tester.v1.shape_report_request"
	TypeShapeReportResponse       = "io.synadia.ntf.tester.v1.shape_report_response"
	TypeShapeListRequest          = "io.synadia.ntf.tester.v1.shape_list_request"
	TypeShapeListResponse         = "io.synadia.ntf.tester.v1.shape_list_response"
	TypeToxicAddRequest           = "io.synadia.ntf.tester.v1.toxic_add_request"
	TypeToxicAddResponse          = "io.synadia.ntf.tester.v1.toxic_add_response"
	TypeToxicRemoveRequest        = "io.synadia.ntf.tester.v1.toxic_remove_request"
	TypeToxicRemoveResponse       = "io.synadia.ntf.tester.v1.toxic_remove_response"
	TypeToxicClearRequest         = "io.synadia.ntf.tester.v1.toxic_clear_request"
	TypeToxicClearResponse        = "io.synadia.ntf.tester.v1.toxic_clear_response"
	TypeToxicReportRequest        = "io.synadia.ntf.tester.v1.toxic_report_request"
	TypeToxicReportResponse       = "io.synadia.ntf.tester.v1.toxic_report_response"
	TypeStatusRequest             = "io.synadia.ntf.tester.v1.status_request"
	TypeStatusResponse            = "io.synadia.ntf.tester.v1.status_response"
	TypeResetRequest              = "io.synadia.ntf.tester.v1.reset_request"
	TypeResetResponse             = "io.synadia.ntf.tester.v1.reset_response"
	TypeDestroyRequest            = "io.synadia.ntf.tester.v1.destroy_request"
	TypeDestroyResponse           = "io.synadia.ntf.tester.v1.destroy_response"
	TypeListRequest               = "io.synadia.ntf.tester.v1.list_request"
	TypeListResponse              = "io.synadia.ntf.tester.v1.list_response"
	TypeClockRequest              = "io.synadia.ntf.tester.v1.clock_request"
	TypeClockResponse             = "io.synadia.ntf.tester.v1.clock_response"
	TypeSchemaRequest             = "io.synadia.ntf.tester.v1.schema_request"
	TypeSchemaResponse            = "io.synadia.ntf.tester.v1.schema_response"
)

// TypeToxicAttributes is the id of the toxic attributes fragment, the rules of
// the attributes each toxic type accepts, which a schema of a toxic reuses by
// its $id. It is no request or response.
const TypeToxicAttributes = "io.synadia.ntf.tester.v1.toxic_attributes"

// Endpoint describes one endpoint of the tester service. Name is relative to
// the service group, such as "create.server" for "tester.create.server", since
// Options.Group can move every subject. Request and Response are the Go types
// of its request and response, and RequestType and ResponseType their type
// ids.
type Endpoint struct {
	Name         string
	Description  string
	Request      reflect.Type
	RequestType  string
	Response     reflect.Type
	ResponseType string
}

// Endpoints lists every endpoint of the tester service, in the order the
// service adds them.
var Endpoints = []Endpoint{
	{"create.server", "Creates an instance of one server", reflect.TypeFor[CreateServerRequest](), TypeCreateServerRequest, reflect.TypeFor[CreateResponse](), TypeCreateResponse},
	{"create.cluster", "Creates an instance of one cluster", reflect.TypeFor[CreateClusterRequest](), TypeCreateClusterRequest, reflect.TypeFor[CreateResponse](), TypeCreateResponse},
	{"create.super-cluster", "Creates an instance of a super-cluster, clusters joined by gateways", reflect.TypeFor[CreateSuperClusterRequest](), TypeCreateSuperClusterRequest, reflect.TypeFor[CreateResponse](), TypeCreateResponse},
	{"stop.server", "Stops one server, keeping its config and storage", reflect.TypeFor[StopServerRequest](), TypeStopServerRequest, reflect.TypeFor[StopServerResponse](), TypeStopServerResponse},
	{"stop.instance", "Stops every server of an instance, keeping their config and storage", reflect.TypeFor[StopInstanceRequest](), TypeStopInstanceRequest, reflect.TypeFor[StopInstanceResponse](), TypeStopInstanceResponse},
	{"start.server", "Starts a stopped server from its config", reflect.TypeFor[StartServerRequest](), TypeStartServerRequest, reflect.TypeFor[StartServerResponse](), TypeStartServerResponse},
	{"start.instance", "Starts every stopped server of an instance from its config", reflect.TypeFor[StartInstanceRequest](), TypeStartInstanceRequest, reflect.TypeFor[StartInstanceResponse](), TypeStartInstanceResponse},
	{"update.server", "Writes a new config for one server, which a reload applies", reflect.TypeFor[UpdateServerRequest](), TypeUpdateServerRequest, reflect.TypeFor[UpdateServerResponse](), TypeUpdateServerResponse},
	{"reload.server", "Reloads the config of a running server", reflect.TypeFor[ReloadServerRequest](), TypeReloadServerRequest, reflect.TypeFor[ReloadServerResponse](), TypeReloadServerResponse},
	{"shape.set", "Adds a shaping set to the capture proxies of an instance, replacing one with the same id", reflect.TypeFor[ShapeSetRequest](), TypeShapeSetRequest, reflect.TypeFor[ShapeSetResponse](), TypeShapeSetResponse},
	{"shape.clear", "Removes one shaping set of an instance, or every set", reflect.TypeFor[ShapeClearRequest](), TypeShapeClearRequest, reflect.TypeFor[ShapeClearResponse](), TypeShapeClearResponse},
	{"shape.report", "Reports the firings of one shaping set of an instance, or of every set", reflect.TypeFor[ShapeReportRequest](), TypeShapeReportRequest, reflect.TypeFor[ShapeReportResponse](), TypeShapeReportResponse},
	{"shape.list", "Lists one shaping set of an instance, or every set, as it was submitted to shape.set", reflect.TypeFor[ShapeListRequest](), TypeShapeListRequest, reflect.TypeFor[ShapeListResponse](), TypeShapeListResponse},
	{"toxic.add", "Adds a toxic to the links of an instance created with toxics", reflect.TypeFor[ToxicAddRequest](), TypeToxicAddRequest, reflect.TypeFor[ToxicAddResponse](), TypeToxicAddResponse},
	{"toxic.remove", "Removes one toxic from an instance", reflect.TypeFor[ToxicRemoveRequest](), TypeToxicRemoveRequest, reflect.TypeFor[ToxicRemoveResponse](), TypeToxicRemoveResponse},
	{"toxic.clear", "Removes every toxic from an instance", reflect.TypeFor[ToxicClearRequest](), TypeToxicClearRequest, reflect.TypeFor[ToxicClearResponse](), TypeToxicClearResponse},
	{"toxic.report", "Reports the links of an instance and the toxics on them", reflect.TypeFor[ToxicReportRequest](), TypeToxicReportRequest, reflect.TypeFor[ToxicReportResponse](), TypeToxicReportResponse},
	{"status", "Reports the status of one instance, or of every instance", reflect.TypeFor[StatusRequest](), TypeStatusRequest, reflect.TypeFor[StatusResponse](), TypeStatusResponse},
	{"reset", "Destroys every instance", reflect.TypeFor[ResetRequest](), TypeResetRequest, reflect.TypeFor[ResetResponse](), TypeResetResponse},
	{"destroy", "Destroys one instance", reflect.TypeFor[DestroyRequest](), TypeDestroyRequest, reflect.TypeFor[DestroyResponse](), TypeDestroyResponse},
	{"list", "Lists every instance", reflect.TypeFor[ListRequest](), TypeListRequest, reflect.TypeFor[ListResponse](), TypeListResponse},
	{"clock", "Reports the clocks of the service, the agents and extra subjects against the service host's clock", reflect.TypeFor[ClockRequest](), TypeClockRequest, reflect.TypeFor[ClockResponse](), TypeClockResponse},
	{"schema", "Reports the JSON Schemas of the request and response of every endpoint", reflect.TypeFor[SchemaRequest](), TypeSchemaRequest, reflect.TypeFor[SchemaResponse](), TypeSchemaResponse},
}

// schemaPath returns the path of the JSON Schema for type id typ, such as
// schemas/ntf/tester/v1/create_server_request.json, inside this package's
// directory. typ is the request or response type of an endpoint in Endpoints,
// or TypeToxicAttributes.
func schemaPath(typ string) (string, error) {
	known := typ == TypeToxicAttributes
	for _, ep := range Endpoints {
		if ep.RequestType == typ || ep.ResponseType == typ {
			known = true
			break
		}
	}
	if !known {
		return "", fmt.Errorf("%w: %q", ErrUnknownType, typ)
	}

	token := strings.TrimPrefix(typ, typePrefix)
	return "schemas/" + strings.ReplaceAll(token, ".", "/") + ".json", nil
}

// Schema returns the JSON Schema document for type id typ, the request or
// response type of an endpoint in Endpoints, or the toxic attributes fragment
// for TypeToxicAttributes.
func Schema(typ string) ([]byte, error) {
	path, err := schemaPath(typ)
	if err != nil {
		return nil, err
	}

	return schemaFS.ReadFile(path)
}
