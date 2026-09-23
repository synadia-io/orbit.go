package ntf

import (
	"context"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// SetShaping applies set to the capture proxy of the instance with the given ID,
// replacing a set with the same ID and resetting its counters. The instance must
// have been created with WithTraceCapture. The service compiles the set's rules
// and refuses one it cannot, naming the rule and field in the ErrService error.
func (m *Manager) SetShaping(ctx context.Context, instanceID string, set api.ShapingSet) (*api.ShapeSetResponse, error) {
	resp := api.ShapeSetResponse{}
	err := m.request(ctx, "tester.shape.set", requestTimeout, api.ShapeSetRequest{InstanceID: instanceID, Set: set}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// ClearShaping removes the shaping set with ID setID from the capture proxy of
// the instance with the given ID, or every set when setID is empty. The response
// lists the IDs of the sets removed.
func (m *Manager) ClearShaping(ctx context.Context, instanceID string, setID string) (*api.ShapeClearResponse, error) {
	resp := api.ShapeClearResponse{}
	err := m.request(ctx, "tester.shape.clear", requestTimeout, api.ShapeClearRequest{InstanceID: instanceID, Set: setID}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// ShapingReport reports every firing of the shaping set with ID setID on the
// capture proxy of the instance with the given ID, or of every set when setID is
// empty.
func (m *Manager) ShapingReport(ctx context.Context, instanceID string, setID string) (*api.ShapeReportResponse, error) {
	resp := api.ShapeReportResponse{}
	err := m.request(ctx, "tester.shape.report", requestTimeout, api.ShapeReportRequest{InstanceID: instanceID, Set: setID}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
