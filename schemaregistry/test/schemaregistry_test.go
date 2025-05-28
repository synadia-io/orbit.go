package tests

import (
	"context"
	"os"
	"schemaregistry"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestAdd(t *testing.T) {

	nc, err := nats.Connect("nats://localhost:4222")
	require_NoError(t, err)

	schema, err := os.ReadFile("test_data/jsonschema.json")
	require_NoError(t, err)

	// Create a schema registry
	schemaRegistry := schemaregistry.NewSchemaRegistry(nc)

	resp, err := schemaRegistry.Add(context.Background(), schemaregistry.AddRequest{
		Name:       "test",
		Definition: string(schema),
		Format:     "jsonschema",
	})

	require_NoError(t, err)
	require_Equal(t, resp.Revision, 1)

	getResp, err := schemaRegistry.Get(context.Background(), schemaregistry.GetRequest{
		Name:     "test",
		Revision: 1,
	})
	require_NoError(t, err)

	require_Equal(t, getResp.Schema.Name, "test")
	require_Equal(t, getResp.Schema.Revision, 1)

	updateResp, err := schemaRegistry.Update(context.Background(), schemaregistry.UpdateRequest{
		Name:        "test",
		Description: "Updated description",
		Definition:  string(schema),
	})
	require_NoError(t, err)

	require_Equal(t, updateResp.Revision, 2)
}
