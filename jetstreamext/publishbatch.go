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
	"context"
	"encoding/json"
	"fmt"
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
	// with the final message which includes a commit header.
	BatchPublisher interface {
		// Add publishes a message to the batch with the given subject and data.
		// It is an IO operation and the message will be published immediately
		// and persisted upon commit.
		Add(subject string, data []byte, opts ...BatchMsgOpt) error

		// AddMsg publishes a message to the batch.
		AddMsg(msg *nats.Msg, opts ...BatchMsgOpt) error

		// Commit publishes the final message with the given subject and data,
		// and commits the batch. Returns a BatchAck containing the acknowledgment
		// from the server.
		Commit(ctx context.Context, subject string, data []byte, opts ...BatchMsgOpt) (*BatchAck, error)

		// CommitMsg publishes the final message and commits the batch.
		// Returns a BatchAck containing the acknowledgment from the server.
		CommitMsg(ctx context.Context, msg *nats.Msg, opts ...BatchMsgOpt) (*BatchAck, error)

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
		BatchID string `json:"batch_id,omitempty"`

		// BatchSize is the number of messages in the batch.
		BatchSize int `json:"batch_size,omitempty"`
	}

	batchPublisher struct {
		js       jetstream.JetStream
		batchID  string
		sequence int
		closed   bool
		opts     batchPublishOpts
		mu       sync.Mutex
	}

	apiResponse struct {
		Type  string              `json:"type"`
		Error *jetstream.APIError `json:"error,omitempty"`
	}

	batchAckResponse struct {
		apiResponse
		*jetstream.PubAck
		BatchID   string `json:"batch,omitempty"`
		BatchSize int    `json:"count,omitempty"`
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

const (
	// BatchIDHeader contains the batch ID for a message in a batch publish.
	BatchIDHeader = "Nats-Batch-Id"

	// BatchSeqHeader contains the sequence number of a message within a batch.
	BatchSeqHeader = "Nats-Batch-Sequence"

	// BatchCommitHeader signals the final message in a batch when set to "1".
	BatchCommitHeader = "Nats-Batch-Commit"
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

// AddMsg publishes a message to the batch.
func (b *batchPublisher) AddMsg(msg *nats.Msg, opts ...BatchMsgOpt) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBatchClosed
	}

	if msg.Header == nil {
		msg.Header = nats.Header{}
	}

	// Process batch message options
	o := batchMsgOpts{}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}

	if o.ttl > 0 {
		msg.Header.Set(jetstream.MsgTTLHeader, o.ttl.String())
	}
	if o.stream != "" {
		msg.Header.Set(jetstream.ExpectedStreamHeader, o.stream)
	}
	if o.lastSubjectSeq != nil {
		msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	}
	if o.lastSubject != "" {
		msg.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, o.lastSubject)
		msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	}
	if o.lastSeq != nil {
		msg.Header.Set(jetstream.ExpectedLastSeqHeader, strconv.FormatUint(*o.lastSeq, 10))
	}

	b.sequence++
	msg.Header.Set(BatchIDHeader, b.batchID)
	msg.Header.Set(BatchSeqHeader, strconv.FormatUint(uint64(b.sequence), 10))

	// Determine if we need flow control for this message
	needsAck := false
	if b.opts.flowControl.AckFirst && b.sequence == 1 {
		needsAck = true // wait on first message
	} else if b.opts.flowControl.AckEvery > 0 && b.sequence%b.opts.flowControl.AckEvery == 0 {
		needsAck = true // periodic flow control
	}

	// If we don't need an ack, use core nats publish
	if !needsAck {
		return b.js.Conn().PublishMsg(msg)
	}

	inbox := b.js.Conn().NewRespInbox()
	msg.Reply = inbox

	resp, err := b.js.Conn().RequestMsg(msg, b.opts.flowControl.AckTimeout)
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

// CommitMsg publishes the final message and commits the batch.
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
	// Process batch message options and convert to PublishOpt
	o := batchMsgOpts{}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}

	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	if o.ttl > 0 {
		msg.Header.Set(jetstream.MsgTTLHeader, o.ttl.String())
	}
	if o.stream != "" {
		msg.Header.Set(jetstream.ExpectedStreamHeader, o.stream)
	}
	if o.lastSubjectSeq != nil {
		msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	}
	if o.lastSubject != "" {
		msg.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, o.lastSubject)
		msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	}
	if o.lastSeq != nil {
		msg.Header.Set(jetstream.ExpectedLastSeqHeader, strconv.FormatUint(*o.lastSeq, 10))
	}

	b.sequence++

	msg.Header.Set(BatchIDHeader, b.batchID)
	msg.Header.Set(BatchSeqHeader, strconv.FormatUint(uint64(b.sequence), 10))
	msg.Header.Set(BatchCommitHeader, "1")

	ctx, cancel = wrapContextWithoutDeadline(ctx, b.js)
	if cancel != nil {
		defer cancel()
	}

	var resp *nats.Msg
	var err error

	resp, err = b.js.Conn().RequestMsgWithContext(ctx, msg)

	if err != nil {
		return nil, err
	}

	b.closed = true

	var batchResp batchAckResponse
	if err := json.Unmarshal(resp.Data, &batchResp); err != nil {
		return nil, jetstream.ErrInvalidJSAck
	}
	if batchResp.Error != nil {
		return nil, batchResp.Error
	}
	if batchResp.PubAck == nil || batchResp.PubAck.Stream == "" ||
		batchResp.BatchID != b.batchID || batchResp.BatchSize != int(b.sequence) {
		return nil, ErrInvalidBatchAck
	}

	return &BatchAck{
		Stream:    batchResp.PubAck.Stream,
		Sequence:  batchResp.PubAck.Sequence,
		Domain:    batchResp.PubAck.Domain,
		BatchID:   batchResp.BatchID,
		BatchSize: batchResp.BatchSize,
	}, nil
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
	var err error
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

	batchID := nuid.Next()

	for i := range messages {
		messages[i].Header.Del(BatchCommitHeader)
		messages[i].Header.Set(BatchIDHeader, batchID)
		messages[i].Header.Set(BatchSeqHeader, strconv.Itoa(i+1))

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
				err = js.Conn().PublishMsg(messages[i])
				if err != nil {
					return nil, fmt.Errorf("publishing message in the batch: %w", err)
				}
				continue
			}
			inbox := js.Conn().NewRespInbox()
			messages[i].Reply = inbox

			resp, err := js.Conn().RequestMsg(messages[i], pubOpts.flowControl.AckTimeout)
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
		messages[i].Header.Set(BatchCommitHeader, "1")

		var resp *nats.Msg
		var err error

		resp, err = js.Conn().RequestMsgWithContext(ctx, messages[i])

		if err != nil {
			return nil, err
		}

		var batchResp batchAckResponse
		if err := json.Unmarshal(resp.Data, &batchResp); err != nil {
			return nil, jetstream.ErrInvalidJSAck
		}
		if batchResp.Error != nil {
			return nil, batchResp.Error
		}
		if batchResp.PubAck == nil || batchResp.PubAck.Stream == "" ||
			batchResp.BatchID != batchID || batchResp.BatchSize != msgs {

			return nil, ErrInvalidBatchAck
		}

		batchAck = &BatchAck{
			Stream:    batchResp.PubAck.Stream,
			Sequence:  batchResp.PubAck.Sequence,
			Domain:    batchResp.PubAck.Domain,
			BatchID:   batchResp.BatchID,
			BatchSize: batchResp.BatchSize,
		}

	}
	return batchAck, nil
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
