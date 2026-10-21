package ntf

import (
	"context"

	"github.com/synadia-io/orbit.go/ntf/api"
)

// AddToxic adds toxic to the links of the instance with the given ID, which
// must have been created with WithToxics. The service's agent resolves the
// toxic's selector to the instance's proxies and validates its type,
// attributes and toxicity, and refuses a toxic it cannot apply, or whose name
// is in use, naming the toxic and the field in the ErrService error. The
// response holds the toxic as applied, with the links it resolved to.
func (m *Manager) AddToxic(ctx context.Context, instanceID string, toxic api.Toxic) (*api.ToxicAddResponse, error) {
	resp := api.ToxicAddResponse{}
	err := m.request(ctx, "tester.toxic.add", requestTimeout, api.ToxicAddRequest{InstanceID: instanceID, Toxic: toxic}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// RemoveToxic removes the toxic named name from the links of the instance with
// the given ID.
func (m *Manager) RemoveToxic(ctx context.Context, instanceID string, name string) (*api.ToxicRemoveResponse, error) {
	resp := api.ToxicRemoveResponse{}
	err := m.request(ctx, "tester.toxic.remove", requestTimeout, api.ToxicRemoveRequest{InstanceID: instanceID, Name: name}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// ClearToxics removes every toxic from the links of the instance with the
// given ID. The response lists their names in the order they were added.
func (m *Manager) ClearToxics(ctx context.Context, instanceID string) (*api.ToxicClearResponse, error) {
	resp := api.ToxicClearResponse{}
	err := m.request(ctx, "tester.toxic.clear", requestTimeout, api.ToxicClearRequest{InstanceID: instanceID}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// ToxicReport reports every proxied link of the instance with the given ID
// with the toxics on it, and every toxic in place.
func (m *Manager) ToxicReport(ctx context.Context, instanceID string) (*api.ToxicReportResponse, error) {
	resp := api.ToxicReportResponse{}
	err := m.request(ctx, "tester.toxic.report", requestTimeout, api.ToxicReportRequest{InstanceID: instanceID}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
