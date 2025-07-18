package counters

import (
	"math/big"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestParseCounterValue(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected *big.Int
		wantErr  bool
	}{
		{
			name:     "valid positive value",
			data:     []byte(`{"val":"42"}`),
			expected: big.NewInt(42),
		},
		{
			name:     "valid negative value",
			data:     []byte(`{"val":"-10"}`),
			expected: big.NewInt(-10),
		},
		{
			name:     "valid zero value",
			data:     []byte(`{"val":"0"}`),
			expected: big.NewInt(0),
		},
		{
			name:     "valid large value",
			data:     []byte(`{"val":"123456789012345678901234567890"}`),
			expected: func() *big.Int { v, _ := new(big.Int).SetString("123456789012345678901234567890", 10); return v }(),
		},
		{
			name:    "invalid JSON",
			data:    []byte(`{"val": invalid}`),
			wantErr: true,
		},
		{
			name:    "missing val field",
			data:    []byte(`{"other":"42"}`),
			wantErr: true,
		},
		{
			name:    "invalid number format",
			data:    []byte(`{"val":"not-a-number"}`),
			wantErr: true,
		},
		{
			name:    "empty data",
			data:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseCounterValue(tt.data)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result.Cmp(tt.expected) != 0 {
				t.Errorf("expected %s, got %s", tt.expected.String(), result.String())
			}
		})
	}
}

func TestParseSources(t *testing.T) {
	tests := []struct {
		name     string
		headers  nats.Header
		expected map[string]map[string]*big.Int
		wantErr  bool
	}{
		{
			name:     "empty headers",
			headers:  nats.Header{},
			expected: nil,
		},
		{
			name: "single source single subject",
			headers: nats.Header{
				"Nats-Counter-Sources": []string{`{"source1":{"subject1":"10"}}`},
			},
			expected: map[string]map[string]*big.Int{
				"source1": {"subject1": big.NewInt(10)},
			},
		},
		{
			name: "single source multiple subjects",
			headers: nats.Header{
				"Nats-Counter-Sources": []string{`{"source1":{"subject1":"10","subject2":"20"}}`},
			},
			expected: map[string]map[string]*big.Int{
				"source1": {"subject1": big.NewInt(10), "subject2": big.NewInt(20)},
			},
		},
		{
			name: "multiple sources",
			headers: nats.Header{
				"Nats-Counter-Sources": []string{`{"source1":{"subject1":"10"},"source2":{"subject2":"20","subject3":"90"}}`},
			},
			expected: map[string]map[string]*big.Int{
				"source1": {"subject1": big.NewInt(10)},
				"source2": {"subject2": big.NewInt(20), "subject3": big.NewInt(90)},
			},
		},
		{
			name: "invalid JSON",
			headers: nats.Header{
				"Nats-Counter-Sources": []string{`{`},
			},
			wantErr: true,
		},
		{
			name: "invalid value format",
			headers: nats.Header{
				"Nats-Counter-Sources": []string{`{"source1":{"subject1":"not-a-number"}}`},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseSources(tt.headers)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if len(result) != len(tt.expected) {
				t.Errorf("expected %d sources, got %d", len(tt.expected), len(result))
				return
			}
			for sourceID, expectedSubjects := range tt.expected {
				actualSubjects, ok := result[sourceID]
				if !ok {
					t.Errorf("missing source %s", sourceID)
					continue
				}
				if len(actualSubjects) != len(expectedSubjects) {
					t.Errorf("source %s: expected %d subjects, got %d", sourceID, len(expectedSubjects), len(actualSubjects))
					continue
				}
				for subject, expectedValue := range expectedSubjects {
					actualValue, ok := actualSubjects[subject]
					if !ok {
						t.Errorf("source %s: missing subject %s", sourceID, subject)
						continue
					}
					if actualValue.Cmp(expectedValue) != 0 {
						t.Errorf("source %s subject %s: expected %s, got %s", sourceID, subject, expectedValue, actualValue)
					}
				}
			}
		})
	}
}
