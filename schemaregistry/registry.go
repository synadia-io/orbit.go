package schemaregistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type SchemaFormat string
type SchemaCompatPolicy string

type Schema struct {
	// TODO(jrm): We probably do not need the ID, as we can use subject + version as a compound unique identifier.
	// Unique identifier of the schema.
	// ID string `json:"id"`
	Version int `json:"version"`

	// Revision of the schema.
	Revision int `json:"revision"`

	// Timestamp when this schema was added.
	Time time.Time `json:"time"`

	// Unique name of the schema.
	Name string `json:"name"`

	// TODO(jrm): This follows Confluent's Schema Registry, but I think we should leverage messages headers without complicating things relating it to the subjects.
	// The subject to bind the schema to.
	// Subject string `json:"subject"`

	// The schema definition.
	Definition string `json:"definition"`

	// The format of the schema.
	// Currently supported is `jsonschema`, `avro`, and `protobuf`.
	Format SchemaFormat `json:"format"`

	// The compatibility policy for this schema.
	// The value can be `none`, `backwards`, `forward`, or `full`.
	// If not set, the default is `backwards`.
	CompatPolicy SchemaCompatPolicy `json:"compat_policy"`

	// Description of the schema.
	Description string `json:"description"`

	// Metadata is a map of key-value pairs.
	Metadata map[string]string `json:"metadata"`

	// Deleted is a flag to indicate if the schema is deleted.
	Deleted bool `json:"delete,omitempty"`
}

type AddRequest struct {
	// Unique name of the schema (required).
	Name string `json:"name"`

	// TODO(jrm): we should validate the passed schema
	// The schema definition (required).
	Definition string `json:"definition"`

	// The format of the schema (required).
	// Currently supported is `jsonschema`, `avro`, and `protobuf`.
	Format SchemaFormat `json:"format"`

	// TODO(jrm): should this be scoper per Schema version, or per Schema name?
	// The compatibility policy for this schema (optional).
	// The value can be `none`, `backwards`, `forward`, or `full`.
	// If not set, the default is `backwards`.
	CompatPolicy SchemaCompatPolicy `json:"compat_policy,omitempty"`

	// Description of the schema (optional).
	Description string `json:"description,omitempty"`

	// Metadata is a map of key-value pairs (optional).
	Metadata map[string]string `json:"metadata,omitempty"`
}

type AddResponse struct {
	Revision int `json:"revision"`
	Version  int `json:"version"`
}

type Added struct {
	Schema *Schema `json:"schema"`
}

type UpdateRequest struct {
	// Name of the schema (required).
	Name string `json:"name"`

	// The schema definition (required).
	Definition *string `json:"definition"`

	// Description of the schema (optional).
	Description *string `json:"description,omitempty"`

	// Metadata is a map of key-value pairs (optional).
	Metadata *map[string]string `json:"metadata,omitempty"`
}

// TODO(jrm): Have proper schemas for responses that include errors, which should also by typed.

type UpdateResponse struct {
	Revision int `json:"revision"`
	Version  int `json:"version"`
}

type RemoveRequest struct {
	// Name of the schema (required).
	Name string `json:"name,omitempty"`

	// Revision of the schema (optional). If not set, the latest revision
	// of the schema will be removed.
	Revision int `json:"revision,omitempty"`

	Version int `json:"version,omitempty"`

	Format *string `json:"format,omitempty"`
}

type RemoveResponse struct{}

type GetRequest struct {
	// Name of the schema (optiona). Must be set if
	Name string `json:"name,omitempty"`

	// Revision of the schema (optional). If not set, the latest version
	// of the schema will be returned.
	Revision int `json:"revision,omitempty"`

	Version int `json:"version,omitempty"`

	Format *string `json:"format,omitempty"`
}

type GetResponse struct {
	Schema *Schema `json:"schema"`
}

type ListRequest struct {
	// SubjectFilter is a filter to list schemas with an overlapping
	// subject (optional).
	SubjectFilter string `json:"filter,omitempty"`
}

type ListResponse struct {
	Schemas []string `json:"schemas"`
}

type ValidateRequest struct {
	// Name of the schema (optional). Must be set if subject is not.
	Name string `json:"name,omitempty"`

	// Revision of the schema (optional). If not set, the latest version
	// of the schema will be used.
	Revision int `json:"revision,omitempty"`

	Version int `json:"version,omitempty"`

	// Subject of the schema. Must be set if name is not.
	Subject string `json:"subject,omitempty"`

	// Data to validate.
	Data []byte `json:"data"`
}

type ValidateResponse struct{}

type ErrorType struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *ErrorType) Error() string {
	// a concise human-readable string
	return fmt.Sprintf("service error %s: %s", e.Code, e.Message)
}

// Optional: so that errors.Is(err, ErrSchemaNotFound) will match by Code
func (e *ErrorType) Is(target error) bool {
	t, ok := target.(*ErrorType)
	return ok && t.Code == e.Code
}

const (
	ErrCodeInvalidRequest = "10400"
	ErrCodeInternalError  = "10500"
)

var (
	ErrNotImplemented = errors.New("not implemented")

	// TODO: clean up errors
	ErrSchemaNameInvalid         = &ErrorType{Code: "11000", Message: "schema name is invalid"}
	ErrSchemaNameAlreadyExists   = &ErrorType{Code: "11001", Message: "schema name already exists"}
	ErrSchemaNotFound            = &ErrorType{Code: "11002", Message: "schema not found"}
	ErrSchemaSubjectInvalid      = &ErrorType{Code: "11002", Message: "schema subject is invalid"}
	ErrSchemaSubjectAlreadyBound = &ErrorType{Code: "11003", Message: "schema subject already bound"}
	ErrSchemaFormatRequired      = &ErrorType{Code: "11004", Message: "schema format is required"}
	ErrSchemaDefinitionInvalid   = &ErrorType{Code: "11005", Message: "schema definition is invalid"}
	ErrParseKey                  = &ErrorType{Code: "11006", Message: "failed to parse key"}
)

// Registry is the interface that wraps the basic methods for a schema registry.
type Registry interface {
	Add(context.Context, *AddRequest) (*AddResponse, error)
	Update(context.Context, *UpdateRequest) (*UpdateResponse, error)
	Remove(context.Context, *RemoveRequest) (*RemoveResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	List(context.Context, *ListRequest) (*ListResponse, error)
	// TODO(jrm): We should decide later what to do with this endpoint, as it can open pandora box of issues.
	Validate(context.Context, *ValidateRequest) (*ValidateResponse, error)
}

func headersToError(headers nats.Header) error {
	if headers == nil {
		return nil
	}

	code := headers.Get("Nats-Service-Error-Code")
	if code == "" {
		return nil
	}

	message := headers.Get("Nats-Service-Error")
	if message == "" {
		message = "unknown error"
	}

	return &ErrorType{
		Code:    code,
		Message: message,
	}
}

type SchemaRegistry struct {
	client *nats.Conn
}

func NewSchemaRegistry(client *nats.Conn) *SchemaRegistry {
	return &SchemaRegistry{
		client: client,
	}
}

func (sr *SchemaRegistry) Close() {
	sr.client.Close()
}

func (sr *SchemaRegistry) Add(ctx context.Context, req AddRequest) (*AddResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := sr.client.RequestWithContext(ctx, fmt.Sprintf("$SR.v1.ADD.%s", req.Name), data)
	if err != nil {
		return nil, err
	}

	var addResp AddResponse
	if err := json.Unmarshal(resp.Data, &addResp); err != nil {
		return nil, err
	}

	err = headersToError(resp.Header)
	if err != nil {
		return nil, err
	}

	return &addResp, nil
}
