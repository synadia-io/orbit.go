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
	"fmt"
	"time"
)

// WithBatchMsgTTL sets per msg TTL for batch messages.
// Requires [StreamConfig.AllowMsgTTL] to be enabled.
func WithBatchMsgTTL(dur time.Duration) BatchMsgOpt {
	return func(opts *batchMsgOpts) error {
		opts.ttl = dur
		return nil
	}
}

// WithBatchExpectStream sets the expected stream the message should be published to.
// If the message is published to a different stream server will reject the
// message and publish will fail.
func WithBatchExpectStream(stream string) BatchMsgOpt {
	return func(opts *batchMsgOpts) error {
		opts.stream = stream
		return nil
	}
}

// WithBatchExpectLastSequence sets the expected sequence number the last message
// on a stream should have. If the last message has a different sequence number
// server will reject the message and publish will fail.
func WithBatchExpectLastSequence(seq uint64) BatchMsgOpt {
	return func(opts *batchMsgOpts) error {
		opts.lastSeq = &seq
		return nil
	}
}

// WithBatchExpectLastSequencePerSubject sets the expected sequence number the last
// message on a subject the message is published to. If the last message on a
// subject has a different sequence number server will reject the message and
// publish will fail.
func WithBatchExpectLastSequencePerSubject(seq uint64) BatchMsgOpt {
	return func(opts *batchMsgOpts) error {
		opts.lastSubjectSeq = &seq
		return nil
	}
}

// WithBatchExpectLastSequenceForSubject sets the sequence and subject for which the
// last sequence number should be checked. If the last message on a subject
// has a different sequence number server will reject the message and publish
// will fail.
func WithBatchExpectLastSequenceForSubject(seq uint64, subject string) BatchMsgOpt {
	return func(opts *batchMsgOpts) error {
		if subject == "" {
			return fmt.Errorf("%w: subject cannot be empty", ErrInvalidOption)
		}
		opts.lastSubjectSeq = &seq
		opts.lastSubject = subject
		return nil
	}
}

func (fc BatchFlowControl) configureBatchPublisher(opts *batchPublishOpts) error {
	opts.flowControl = fc
	return nil
}

func (fc BatchFlowControl) configurePublishMsgBatch(opts *batchPublishOpts) error {
	opts.flowControl = fc
	return nil
}
