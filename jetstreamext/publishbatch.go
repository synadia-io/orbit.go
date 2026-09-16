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

package jetstreamext

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

type (
	// BatchPublisher provides methods for publishing messages to a stream in batches.
	// Messages are published immediately with batch headers, and the batch is committed
	// either with a final stored message which includes a commit header (Commit and
	// CommitMsg), or with an end-of-batch marker which is not stored (Close).
	BatchPublisher interface {
		// Add publishes a message to the batch with the given subject and data.
		// It is an IO operation and the message will be published immediately
		// and persisted upon commit.
		Add(subject string, data []byte, opts ...BatchMsgOpt) error

		// AddMsg publishes a message to the batch. The message is not modified;
		// batch and option headers are set on a copy.
		AddMsg(msg *nats.Msg, opts ...BatchMsgOpt) error

		// Commit publishes the final message with the given subject and data,
		// and commits the batch. Returns a BatchAck containing the acknowledgment
		// from the server.
		Commit(ctx context.Context, subject string, data []byte, opts ...BatchMsgOpt) (*BatchAck, error)

		// CommitMsg publishes the final message and commits the batch.
		// Returns a BatchAck containing the acknowledgment from the server.
		// The message is not modified.
		CommitMsg(ctx context.Context, msg *nats.Msg, opts ...BatchMsgOpt) (*BatchAck, error)

		// Close commits the batch without storing a final message.
		// It sends an end-of-batch marker to the server, which commits the
		// messages already added. The marker itself is not persisted, and
		// the server updates the last stored message to carry the regular
		// commit header.
		// Returns a BatchAck containing the acknowledgment from the server.
		//
		// Note that Close commits the batch. To abandon a batch without
		// committing it, use Discard instead.
		//
		// Requires nats-server v2.14.0 or later.
		Close(ctx context.Context) (*BatchAck, error)

		// Discard cancels the batch without committing.
		// The server will abandon the batch after a timeout.
		Discard() error

		// Size returns the number of messages added to the batch so far.
		Size() int

		// IsClosed returns true if the batch has been committed or discarded.
		IsClosed() bool
	}

	// BatchFlowControl configures flow control for batch publishing.
	BatchFlowControl struct {
		// AckFirst waits for an ack on the first message in the batch.
		// Default: true
		AckFirst bool

		// AckEvery waits for an ack every N messages (0 = disabled).
		// Default: 0
		AckEvery int

		// AckTimeout is the timeout for waiting for acks when flow control is enabled.
		// Default: timeout from JetStream context.
		AckTimeout time.Duration
	}

	// BatchPublisherOpt is a functional option for configuring a BatchPublisher.
	BatchPublisherOpt interface {
		configureBatchPublisher(*batchPublishOpts) error
	}

	// PublishMsgBatchOpt is a functional option for configuring PublishMsgBatch.
	PublishMsgBatchOpt interface {
		configurePublishMsgBatch(*batchPublishOpts) error
	}

	batchPublishOpts struct {
		flowControl BatchFlowControl
	}

	// BatchAck is the acknowledgment for a batch publish operation.
	BatchAck struct {
		// Stream is the stream name the message was published to.
		Stream string `json:"stream"`

		// Sequence is the stream sequence number of the message.
		Sequence uint64 `json:"seq"`

		// Domain is the domain the message was published to.
		Domain string `json:"domain,omitempty"`

		// Value is the counter value for the stream.
		// This is only set when publishing to a stream with [StreamConfig.AllowMsgCounter] enabled.
		Value string `json:"val,omitempty"`

		// BatchID is the unique identifier for the batch.
		BatchID string `json:"batch,omitempty"`

		// BatchSize is the number of messages in the batch.
		BatchSize uint64 `json:"count,omitempty"`
	}

	batchPublisher struct {
		js       jetstream.JetStream
		batchID  string
		sequence uint64
		closed   bool
		opts     batchPublishOpts
		// batchSubject is the subject of the first message added to the
		// batch, reused to publish the end-of-batch marker in Close.
		batchSubject string
		mu           sync.Mutex
	}

	apiResponse struct {
		Type  string              `json:"type"`
		Error *jetstream.APIError `json:"error,omitempty"`
	}

	batchAckResponse struct {
		apiResponse
		*BatchAck
	}

	// BatchMsgOpt is an option for configuring batch message publishing.
	BatchMsgOpt func(*batchMsgOpts) error

	batchMsgOpts struct {
		ttl            time.Duration
		stream         string
		lastSeq        *uint64
		lastSubjectSeq *uint64
		lastSubject    string
	}
)

// applyBatchMsgOpts sets the headers requested by opts on m. m must be a
// copy of the caller's message whose header map is safe to write to (see
// cloneHeader), so the caller's message is never modified.
func applyBatchMsgOpts(m *nats.Msg, opts []BatchMsgOpt) error {
	var o batchMsgOpts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}
	if o.ttl > 0 {
		m.Header.Set(jetstream.MsgTTLHeader, o.ttl.String())
	}
	if o.stream != "" {
		m.Header.Set(jetstream.ExpectedStreamHeader, o.stream)
	}
	if o.lastSubject != "" {
		m.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, o.lastSubject)
		m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	} else if o.lastSubjectSeq != nil {
		m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	}
	if o.lastSeq != nil {
		m.Header.Set(jetstream.ExpectedLastSeqHeader, strconv.FormatUint(*o.lastSeq, 10))
	}
	return nil
}

// cloneHeader returns a header that can be written to without touching the
// caller's message. A nil header yields an empty one.
func cloneHeader(hdr nats.Header) nats.Header {
	if hdr == nil {
		return nats.Header{}
	}
	return maps.Clone(hdr)
}

// validateBatchMsgHeaders rejects headers the server would refuse only at
// commit time, after the whole batch has already been sent, or would silently
// act on. first reports whether this is the first message of the batch, add
// whether the message is being added rather than used to commit.
func validateBatchMsgHeaders(hdr nats.Header, first, add bool) error {
	if !first && hdr.Get(jetstream.ExpectedLastSeqHeader) != "" {
		return ErrBatchExpectedLastSeqNotFirst
	}
	if add && hdr.Get(BatchCommitHeader) != "" {
		return ErrBatchCommitOnAdd
	}
	return nil
}

const (
	// BatchIDHeader contains the batch ID for a message in a batch publish.
	BatchIDHeader = "Nats-Batch-Id"

	// BatchSeqHeader contains the sequence number of a message within a batch.
	BatchSeqHeader = "Nats-Batch-Sequence"

	// BatchCommitHeader signals the final message in a batch when set to "1".
	BatchCommitHeader = "Nats-Batch-Commit"

	// BatchCommitEOB is the value of BatchCommitHeader signaling an
	// end-of-batch marker: the batch is committed and the marker message
	// itself is not stored. Requires nats-server v2.14.0 or later.
	BatchCommitEOB = "eob"
)

// NewBatchPublisher creates a new batch publisher for publishing messages in batches.
func NewBatchPublisher(js jetstream.JetStream, opts ...BatchPublisherOpt) (BatchPublisher, error) {
	jsOpts := js.Options()
	pubOpts := batchPublishOpts{
		// Set defaults
		flowControl: BatchFlowControl{
			AckFirst:   true,
			AckTimeout: jsOpts.DefaultTimeout,
		},
	}

	for _, opt := range opts {
		if err := opt.configureBatchPublisher(&pubOpts); err != nil {
			return nil, err
		}
	}

	return &batchPublisher{
		js:      js,
		batchID: nuid.Next(),
		opts:    pubOpts,
	}, nil
}

// Add publishes a message to the batch with the given subject and data.
// It is an IO operation and the message will be published immediately
// and persisted upon commit.
func (b *batchPublisher) Add(subject string, data []byte, opts ...BatchMsgOpt) error {
	return b.AddMsg(&nats.Msg{Subject: subject, Data: data}, opts...)
}

// AddMsg publishes a message to the batch. The message is not modified.
func (b *batchPublisher) AddMsg(msg *nats.Msg, opts ...BatchMsgOpt) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBatchClosed
	}

	// Work on a copy so the caller's message is never modified.
	m := *msg
	m.Header = cloneHeader(msg.Header)
	if err := applyBatchMsgOpts(&m, opts); err != nil {
		return err
	}

	if err := validateBatchMsgHeaders(m.Header, b.sequence == 0, true); err != nil {
		return err
	}

	b.sequence++
	if b.batchSubject == "" {
		b.batchSubject = m.Subject
	}
	m.Header.Set(BatchIDHeader, b.batchID)
	m.Header.Set(BatchSeqHeader, strconv.FormatUint(b.sequence, 10))

	// Determine if we need flow control for this message
	var needsAck bool
	if b.opts.flowControl.AckFirst && b.sequence == 1 {
		needsAck = true // wait on first message
	} else if b.opts.flowControl.AckEvery > 0 && b.sequence%uint64(b.opts.flowControl.AckEvery) == 0 {
		needsAck = true // periodic flow control
	}

	// If we don't need an ack, use core nats publish
	if !needsAck {
		return b.js.Conn().PublishMsg(&m)
	}

	resp, err := b.js.Conn().RequestMsg(&m, b.opts.flowControl.AckTimeout)
	if err != nil {
		return fmt.Errorf("batch message %d ack failed: %w", b.sequence, err)
	}

	// for flow control we expect no response data, just an ack
	if len(resp.Data) > 0 {
		var apiResp apiResponse
		if err := json.Unmarshal(resp.Data, &apiResp); err != nil {
			return err
		}
		if apiResp.Error != nil {
			return apiResp.Error
		}
	}

	return nil
}

// Commit publishes the final message and commits the batch.
func (b *batchPublisher) Commit(ctx context.Context, subject string, data []byte, opts ...BatchMsgOpt) (*BatchAck, error) {
	return b.CommitMsg(ctx, &nats.Msg{Subject: subject, Data: data}, opts...)
}

// CommitMsg publishes the final message and commits the batch. The message
// is not modified.
func (b *batchPublisher) CommitMsg(ctx context.Context, msg *nats.Msg, opts ...BatchMsgOpt) (*BatchAck, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx, b.js)
	if cancel != nil {
		defer cancel()
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrBatchClosed
	}

	// Work on a copy so the caller's message is never modified.
	m := *msg
	m.Header = cloneHeader(msg.Header)
	if err := applyBatchMsgOpts(&m, opts); err != nil {
		return nil, err
	}

	if err := validateBatchMsgHeaders(m.Header, b.sequence == 0, false); err != nil {
		return nil, err
	}

	b.sequence++
	m.Header.Set(BatchIDHeader, b.batchID)
	m.Header.Set(BatchSeqHeader, strconv.FormatUint(b.sequence, 10))
	m.Header.Set(BatchCommitHeader, "1")

	resp, err := b.js.Conn().RequestMsgWithContext(ctx, &m)
	if err != nil {
		return nil, err
	}

	b.closed = true

	return parseBatchAck(resp, b.batchID, b.sequence)
}

// Close commits the batch without storing a final message.
// It sends an end-of-batch marker to the server, which commits the messages
// already added to the batch. The marker itself is not persisted, and the
// last stored message is updated by the server to carry the regular commit
// header. Requires nats-server v2.14.0 or later.
func (b *batchPublisher) Close(ctx context.Context) (*BatchAck, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx, b.js)
	if cancel != nil {
		defer cancel()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrBatchClosed
	}
	// Without a recorded subject there is no message the server accepted from
	// this publisher, and no valid subject to send the marker to.
	if b.sequence == 0 || b.batchSubject == "" {
		return nil, ErrEmptyBatch
	}

	// The marker takes the next batch sequence, but is not stored and does
	// not count towards the batch size reported in the ack.
	msg := nats.NewMsg(b.batchSubject)
	msg.Header.Set(BatchIDHeader, b.batchID)
	msg.Header.Set(BatchSeqHeader, strconv.FormatUint(b.sequence+1, 10))
	msg.Header.Set(BatchCommitHeader, BatchCommitEOB)

	resp, err := b.js.Conn().RequestMsgWithContext(ctx, msg)
	if err != nil {
		return nil, err
	}

	b.closed = true

	return parseBatchAck(resp, b.batchID, b.sequence)
}

// Discard cancels the batch without committing.
// Server will abandon the batch after a timeout.
func (b *batchPublisher) Discard() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBatchClosed
	}

	b.closed = true
	return nil
}

// Size returns the number of messages added to the batch so far.
// Note: the return type is int to satisfy the BatchPublisher interface; the internal counter is
// uint64, so batches exceeding math.MaxInt will be reported incorrectly.
func (b *batchPublisher) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int(b.sequence)
}

// IsClosed returns true if the batch has been committed or discarded.
func (b *batchPublisher) IsClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// PublishMsgBatch publishes a batch of messages to a Stream and waits for an ack for the commit.
func PublishMsgBatch(ctx context.Context, js jetstream.JetStream, messages []*nats.Msg, opts ...PublishMsgBatchOpt) (*BatchAck, error) {
	var batchAck *BatchAck
	msgs := len(messages)

	if msgs == 0 {
		return nil, fmt.Errorf("no messages to publish")
	}

	ctx, cancel := wrapContextWithoutDeadline(ctx, js)
	if cancel != nil {
		defer cancel()
	}

	jsOpts := js.Options()
	pubOpts := batchPublishOpts{
		flowControl: BatchFlowControl{
			AckFirst:   true,
			AckTimeout: jsOpts.DefaultTimeout,
		},
	}

	for _, opt := range opts {
		if err := opt.configurePublishMsgBatch(&pubOpts); err != nil {
			return nil, err
		}
	}

	for i := range messages {
		if err := validateBatchMsgHeaders(messages[i].Header, i == 0, false); err != nil {
			return nil, err
		}
	}

	batchID := nuid.Next()

	for i := range messages {
		// Work on a copy so the caller's messages are never modified.
		m := *messages[i]
		m.Header = cloneHeader(messages[i].Header)
		m.Header.Del(BatchCommitHeader)
		m.Header.Set(BatchIDHeader, batchID)
		m.Header.Set(BatchSeqHeader, strconv.Itoa(i+1))

		// add all but last message to the batch
		if i < msgs-1 {
			// Determine if we need flow control for this message
			needsAck := false
			seq := i + 1
			if pubOpts.flowControl.AckFirst && seq == 1 {
				needsAck = true
			} else if pubOpts.flowControl.AckEvery > 0 && seq%pubOpts.flowControl.AckEvery == 0 {
				needsAck = true
			}

			if !needsAck {
				if err := js.Conn().PublishMsg(&m); err != nil {
					return nil, fmt.Errorf("publishing message in the batch: %w", err)
				}
				continue
			}

			resp, err := js.Conn().RequestMsg(&m, pubOpts.flowControl.AckTimeout)
			if err != nil {
				return nil, fmt.Errorf("batch message %d ack failed: %w", seq, err)
			}

			if len(resp.Data) > 0 {
				var apiResp apiResponse
				if err := json.Unmarshal(resp.Data, &apiResp); err == nil && apiResp.Error != nil {
					return nil, apiResp.Error
				}
			}

			continue
		}

		// Commit the batch on the last message.
		m.Header.Set(BatchCommitHeader, "1")

		resp, err := js.Conn().RequestMsgWithContext(ctx, &m)
		if err != nil {
			return nil, err
		}

		batchAck, err = parseBatchAck(resp, batchID, uint64(msgs))
		if err != nil {
			return nil, err
		}
	}
	return batchAck, nil
}

// parseBatchAck unmarshals and validates a batch publish acknowledgement.
// expectedSize is the number of messages the batch is expected to contain,
// which excludes an end-of-batch marker if one was used to commit.
func parseBatchAck(resp *nats.Msg, batchID string, expectedSize uint64) (*BatchAck, error) {
	var batchResp batchAckResponse
	if err := json.Unmarshal(resp.Data, &batchResp); err != nil {
		return nil, jetstream.ErrInvalidJSAck
	}
	if batchResp.Error != nil {
		return nil, batchResp.Error
	}
	if batchResp.BatchAck == nil || batchResp.Stream == "" ||
		batchResp.BatchID != batchID || batchResp.BatchSize != expectedSize {
		return nil, ErrInvalidBatchAck
	}

	return batchResp.BatchAck, nil
}

// wrapContextWithoutDeadline wraps context without deadline with default timeout.
// If deadline is already set, it will be returned as is, and cancel() will be nil.
// Caller should check if cancel() is nil before calling it.
func wrapContextWithoutDeadline(ctx context.Context, js jetstream.JetStream) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, nil
	}
	opts := js.Options()
	return context.WithTimeout(ctx, opts.DefaultTimeout)
}
