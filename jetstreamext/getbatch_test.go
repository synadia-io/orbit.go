// Copyright 2025 Synadia Communications Inc.
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

package jetstreamext

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestConvertDirectGetMsgResponseToMsg(t *testing.T) {
	tests := []struct {
		name    string
		msg     *nats.Msg
		withErr error
	}{
		{
			name: "valid message",
			msg: &nats.Msg{
				Header: nats.Header{
					"Nats-Num-Pending":       []string{"1"},
					jetstream.StreamHeader:   []string{"test-stream"},
					jetstream.SequenceHeader: []string{"1"},
					jetstream.TimeStampHeaer: []string{time.Now().Format(time.RFC3339Nano)},
					jetstream.SubjectHeader:  []string{"test-subject"},
				},
				Data: []byte("test-data"),
			},
		},
		{
			name: "no messages",
			msg: &nats.Msg{
				Header: nats.Header{
					statusHdr: []string{statusNoMessages},
				},
			},
			withErr: ErrNoMessages,
		},
		{
			name: "missing headers",
			msg: &nats.Msg{
				Header: nats.Header{},
			},
			withErr: ErrInvalidResponse,
		},
		{
			name: "missing Nats-Num-Pending header",
			msg: &nats.Msg{
				Header: nats.Header{
					jetstream.StreamHeader:   []string{"test-stream"},
					jetstream.SequenceHeader: []string{"1"},
					jetstream.TimeStampHeaer: []string{time.Now().Format(time.RFC3339Nano)},
					jetstream.SubjectHeader:  []string{"test-subject"},
				},
			},
			withErr: ErrBatchUnsupported,
		},
		{
			name: "missing stream header",
			msg: &nats.Msg{
				Header: nats.Header{
					"Nats-Num-Pending":       []string{"1"},
					jetstream.SequenceHeader: []string{"1"},
					jetstream.TimeStampHeaer: []string{time.Now().Format(time.RFC3339Nano)},
					jetstream.SubjectHeader:  []string{"test-subject"},
				},
			},
			withErr: ErrInvalidResponse,
		},
		{
			name: "missing sequence header",
			msg: &nats.Msg{
				Header: nats.Header{
					"Nats-Num-Pending":       []string{"1"},
					jetstream.StreamHeader:   []string{"test-stream"},
					jetstream.TimeStampHeaer: []string{time.Now().Format(time.RFC3339Nano)},
					jetstream.SubjectHeader:  []string{"test-subject"},
				},
			},
			withErr: ErrInvalidResponse,
		},
		{
			name: "invalid sequence header",
			msg: &nats.Msg{
				Header: nats.Header{
					"Nats-Num-Pending":       []string{"1"},
					jetstream.StreamHeader:   []string{"test-stream"},
					jetstream.SequenceHeader: []string{"invalid-sequence"},
					jetstream.TimeStampHeaer: []string{time.Now().Format(time.RFC3339Nano)},
					jetstream.SubjectHeader:  []string{"test-subject"},
				},
			},
			withErr: ErrInvalidResponse,
		},
		{
			name: "missing timestamp header",
			msg: &nats.Msg{
				Header: nats.Header{
					"Nats-Num-Pending":       []string{"1"},
					jetstream.StreamHeader:   []string{"test-stream"},
					jetstream.SequenceHeader: []string{"1"},
					jetstream.SubjectHeader:  []string{"test-subject"},
				},
			},
			withErr: ErrInvalidResponse,
		},
		{
			name: "invalid timestamp header",
			msg: &nats.Msg{
				Header: nats.Header{
					"Nats-Num-Pending":       []string{"1"},
					jetstream.StreamHeader:   []string{"test-stream"},
					jetstream.SequenceHeader: []string{"1"},
					jetstream.TimeStampHeaer: []string{"invalid-timestamp"},
					jetstream.SubjectHeader:  []string{"test-subject"},
				},
			},
			withErr: ErrInvalidResponse,
		},
		{
			name: "missing subject header",
			msg: &nats.Msg{
				Header: nats.Header{
					"Nats-Num-Pending":       []string{"1"},
					jetstream.StreamHeader:   []string{"test-stream"},
					jetstream.SequenceHeader: []string{"1"},
					jetstream.TimeStampHeaer: []string{time.Now().Format(time.RFC3339Nano)},
				},
			},
			withErr: ErrInvalidResponse,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := convertDirectGetMsgResponseToMsg(test.msg)
			if test.withErr != nil {
				if !errors.Is(err, test.withErr) {
					t.Fatalf("Expected error %v, got %v", test.withErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Received an error on Request test: %s", err)
			}
		})
	}
}

func TestGetPrefixedSubject(t *testing.T) {
	tests := []struct {
		name     string
		jsOpts   jetstream.JetStreamOptions
		subject  string
		expected string
	}{
		{
			name: "with APIPrefix without dot",
			jsOpts: jetstream.JetStreamOptions{
				APIPrefix: "API",
			},
			subject:  "DIRECT.GET.TEST",
			expected: "API.DIRECT.GET.TEST",
		},
		{
			name: "with APIPrefix with dot",
			jsOpts: jetstream.JetStreamOptions{
				APIPrefix: "API.",
			},
			subject:  "DIRECT.GET.TEST",
			expected: "API.DIRECT.GET.TEST",
		},
		{
			name: "with Domain",
			jsOpts: jetstream.JetStreamOptions{
				Domain: "DOMAIN",
			},
			subject:  "DIRECT.GET.TEST",
			expected: "$JS.DOMAIN.DIRECT.GET.TEST",
		},
		{
			name:     "default prefix",
			jsOpts:   jetstream.JetStreamOptions{},
			subject:  "DIRECT.GET.TEST",
			expected: "$JS.API.DIRECT.GET.TEST",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := getPrefixedSubject(test.jsOpts, test.subject)
			if result != test.expected {
				t.Errorf("getPrefixedSubject() = %v, want %v", result, test.expected)
			}
		})
	}
}
