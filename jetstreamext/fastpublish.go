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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type (
	// FastPublisher manages a fast-ingest batch publish session.
	//
	// A FastPublisher is NOT safe for concurrent use. All calls to Add, AddMsg,
	// Commit, CommitMsg, and Close must be made from a single goroutine.
	FastPublisher interface {
		// Add publishes a message to the batch with the given subject and data.
		// It is an IO operation and the message will be published immediately.
		// Add will wait for an ack on the first message published.
		// If the number of outstanding acks exceeds the configured limit,
		// Add will block until an ack is received or the ack timeout is reached.
		// If a gap is detected and continueOnGap is false, the batch will be closed
		// and no further messages can be added.
		// If a gap is detected and continueOnGap is true, the batch will continue
		// to accept messages, but the user will be notified via the error handler.
		Add(subject string, data []byte, opts ...BatchMsgOpt) (*FastPubAck, error)

		// AddMsg publishes a message to the batch.
		AddMsg(msg *nats.Msg, opts ...BatchMsgOpt) (*FastPubAck, error)

		// Commit publishes the final message with the given subject and data,
		// and commits the batch. Returns a BatchAck containing the acknowledgment
		// from the server.
		Commit(ctx context.Context, subject string, data []byte, opts ...BatchMsgOpt) (*BatchAck, error)

		// CommitMsg publishes the final message and commits the batch.
		// Returns a BatchAck containing the acknowledgment from the server.
		CommitMsg(ctx context.Context, msg *nats.Msg, opts ...BatchMsgOpt) (*BatchAck, error)

		// Close closes the batch, signaling the server that no more messages will be added.
		// It sends an EOB commit to the server, without adding a message.
		Close(ctx context.Context) (*BatchAck, error)

		// IsClosed returns true if the batch has been committed or discarded.
		IsClosed() bool
	}
	// FastPublishFlowControl configures flow control for fast batch publishing.
	FastPublishFlowControl struct {
		// Flow is the initial flow control value (ack frequency by message count).
		// Server may adjust this dynamically via flow ack responses.
		// Default: 100
		Flow uint16

		// MaxOutstandingAcks limits how many unacknowledged messages before stalling.
		// When this limit is reached, Add/AddMsg will block until acks are received.
		// Default: 2
		MaxOutstandingAcks uint16

		// AckTimeout is the timeout for waiting on acks.
		// Default: JetStream context default timeout
		AckTimeout time.Duration
	}

	batchFlowAck struct {
		// Type: "ack"
		Type string `json:"type"`
		// Sequence is the sequence of the message that triggered the ack.
		// If "gap: fail" this means the messages up to and including Sequence were persisted.
		// If "gap: ok" this means _some_ of the messages up to and including Sequence were persisted.
		// But there could have been gaps.
		Sequence uint64 `json:"seq"`
		// AckMessages indicates acknowledgements will be sent every N messages.
		Messages uint16 `json:"msgs"`
	}

	// batchFlowGap is used for reporting gaps when fast batch publishing into a stream.
	// This message is purely informational and could technically be lost without the client receiving it.
	batchFlowGap struct {
		// Type: "gap"
		Type string `json:"type"`
		// ExpectedLastSequence is the sequence expected to be received next.
		// Messages starting from ExpectedLastSequence up to (but not including) CurrentSequence were lost.
		ExpectedLastSequence uint64 `json:"last_seq"`
		// CurrentSequence is the sequence of the message that just came in and detected the gap.
		CurrentSequence uint64 `json:"seq"`
	}

	// batchFlowErr is used for reporting errors when fast batch publishing into a stream.
	// This message is purely informational and could technically be lost without the client receiving it.
	batchFlowErr struct {
		// Type: "err"
		Type string `json:"type"`
		// Sequence is the sequence of the message that triggered the error.
		// There are no (relative) guarantees whatsoever about whether the messages up to this sequence were persisted.
		Sequence uint64 `json:"seq"`
		// Error is used to return the error for the Sequence.
		Error *jetstream.APIError `json:"error"`
	}

	// FastPublisherOpt is a functional option for configuring a FastPublisher.
	FastPublisherOpt interface {
		configureFastPublisher(*fastPublisherOpts) error
	}

	fastPublisher struct {
		js             jetstream.JetStream
		flow           uint16
		ackInboxPrefix string
		// replyPrefix caches "<inbox>.<flow>.<gap>." fixed at initialization; not rebuilt when flow changes.
		replyPrefix  string
		ackSub       *nats.Subscription
		sequence     uint64
		ackSequence  uint64
		closed       bool
		opts         fastPublisherOpts
		stallCh      chan struct{}
		commitCh     chan *batchAckResponse
		firstAckCh   chan *batchFlowAck
		initialErrCh chan error
		errHandler   FastPublishErrHandler
		// stored to use when closing batch (EOB commit)
		batchSubject string
		mu           sync.Mutex
	}

	FastPublishErrHandler func(error)

	fastPublisherOpts struct {
		continueOnGap      bool
		flow               uint16
		maxOutstandingAcks uint16
		ackTimeout         time.Duration
		errHandler         FastPublishErrHandler
	}
)

const (
	defaultFastFlow           = 100
	defaultMaxOutstandingAcks = 2
)

const (
	fastBatchStart = iota
	fastBatchAddMsg
	fastBatchCommitMsg
	fastBatchCommitEOB
	fastBatchPing
)

func NewFastPublisher(js jetstream.JetStream, opts ...FastPublisherOpt) (FastPublisher, error) {
	jsOpts := js.Options()

	// Start with defaults
	pubOpts := fastPublisherOpts{
		flow:               defaultFastFlow,
		maxOutstandingAcks: defaultMaxOutstandingAcks,
		ackTimeout:         jsOpts.DefaultTimeout,
	}

	// Apply all options
	for _, opt := range opts {
		if err := opt.configureFastPublisher(&pubOpts); err != nil {
			return nil, err
		}
	}

	fp := &fastPublisher{
		js:             js,
		ackInboxPrefix: js.Conn().NewInbox(),
		flow:           pubOpts.flow,
		opts:           pubOpts,
		errHandler:     pubOpts.errHandler,
	}

	gap := "fail"
	if fp.opts.continueOnGap {
		gap = "ok"
	}
	fp.replyPrefix = fp.ackInboxPrefix + "." + strconv.FormatUint(uint64(fp.flow), 10) + "." + gap + "."

	return fp, nil
}

// sendPing publishes a ping message (op=4) to recover potentially lost acks.
// Ping reuses the highest batch sequence already sent
// and the server responds with the latest flow ack.
// Must be called WITHOUT holding fp.mu.
func (fp *fastPublisher) sendPing() error {
	fp.mu.Lock()
	reply := fp.buildReplySubject(fp.sequence, fastBatchPing)
	subject := fp.batchSubject
	fp.mu.Unlock()

	return fp.js.Conn().PublishRequest(subject, reply, nil)
}

// waitForStall blocks until the stall channel is signaled, sending periodic
// pings to recover lost acks. Returns an error only on total timeout.
// The deadline is intentionally shared across all re-stall cycles: ackTimeout is a
// hard ceiling on total wait time, not per-ack wait time.
// Must be called WITHOUT holding fp.mu.
func (fp *fastPublisher) waitForStall(stallCh <-chan struct{}) error {
	deadline, ping, pingInterval := fp.newPingTimers()
	defer deadline.Stop()
	defer ping.Stop()

	for {
		select {
		case <-stallCh:
			// After receiving a new ack, it could be we still need to wait more if we're being slowed down.
			fp.mu.Lock()
			waitForAck := fp.waitForAck()
			fp.mu.Unlock()
			if waitForAck {
				continue
			}
			return nil
		case <-ping.C:
			if err := fp.sendPing(); err != nil {
				return fmt.Errorf("ping failed: %w", err)
			}
			ping.Reset(pingInterval)
		case <-deadline.C:
			return errors.New("ack timeout")
		}
	}
}

func (fp *fastPublisher) newPingTimers() (deadline, ping *time.Timer, pingInterval time.Duration) {
	// wait for commit ack with periodic pings to recover lost acks, or deadline/context cancel
	pingInterval = fp.opts.ackTimeout / 3
	deadline = time.NewTimer(fp.opts.ackTimeout)
	ping = time.NewTimer(pingInterval)
	return
}

// buildReplySubject constructs the full reply subject using the cached prefix.
func (fp *fastPublisher) buildReplySubject(seq uint64, operation int) string {
	return fp.replyPrefix + strconv.FormatUint(seq, 10) + "." + strconv.FormatInt(int64(operation), 10) + ".$FI"
}

func (fp *fastPublisher) Add(subject string, data []byte, opts ...BatchMsgOpt) (*FastPubAck, error) {
	return fp.AddMsg(&nats.Msg{
		Subject: subject,
		Data:    data,
	}, opts...)
}

type FastPubAck struct {
	// BatchSequence is the sequence of this message within the current batch.
	BatchSequence uint64
	// AckSequence is the highest sequence within the current batch that has been acknowledged.
	// This can be used by application code to release resources of the messages it might want to otherwise retry.
	// If "gap: fail" is used this means all messages below and including this sequence were persisted.
	// If "gap: ok" is used there's no guarantee that all messages were persisted.
	AckSequence uint64
}

func (fp *fastPublisher) AddMsg(msg *nats.Msg, opts ...BatchMsgOpt) (*FastPubAck, error) {
	fp.mu.Lock()

	if fp.closed {
		fp.mu.Unlock()
		return nil, ErrBatchClosed
	}

	if err := applyBatchMsgOpts(msg, opts); err != nil {
		fp.mu.Unlock()
		return nil, err
	}

	fp.sequence++
	operation := fastBatchAddMsg
	if fp.sequence == 1 {
		operation = fastBatchStart
	}
	msg.Reply = fp.buildReplySubject(fp.sequence, operation)

	// Create subscription and handle first message specially
	if fp.sequence == 1 {
		ackSub, err := fp.js.Conn().Subscribe(fmt.Sprintf("%s.>", fp.ackInboxPrefix), fp.ackMsgHandler)
		if err != nil {
			fp.mu.Unlock()
			return nil, err
		}
		fp.ackSub = ackSub

		// Create channel to receive first ack
		firstAckCh := make(chan *batchFlowAck, 1)
		fp.firstAckCh = firstAckCh
		initialErrCh := make(chan error, 1)
		fp.initialErrCh = initialErrCh
		fp.stallCh = make(chan struct{}, 1)

		// Publish with reply inbox already set (without holding lock)
		if err := fp.js.Conn().PublishMsg(msg); err != nil {
			fp.ackSub.Unsubscribe()
			fp.ackSub = nil
			fp.firstAckCh = nil
			fp.initialErrCh = nil
			fp.closed = true
			fp.mu.Unlock()
			return nil, fmt.Errorf("batch message %d publish failed: %w", fp.sequence, err)
		}

		// Release lock to enable ack handler to run
		ackTimer := time.NewTimer(fp.opts.ackTimeout)
		defer ackTimer.Stop()
		fp.mu.Unlock()
		select {
		case firstAck := <-firstAckCh:
			// Re-acquire lock to update state
			fp.mu.Lock()
			defer fp.mu.Unlock()

			fp.batchSubject = msg.Subject
			return &FastPubAck{
				BatchSequence: fp.sequence,
				AckSequence:   firstAck.Sequence,
			}, nil

		case err := <-fp.initialErrCh:
			fp.mu.Lock()
			defer fp.mu.Unlock()
			fp.firstAckCh = nil
			fp.initialErrCh = nil
			fp.closed = true
			fp.ackSub.Unsubscribe()
			fp.ackSub = nil
			return nil, fmt.Errorf("batch message %d ack error: %w", fp.sequence, err)
		case <-ackTimer.C:
			// Re-acquire lock to mark closed
			fp.mu.Lock()
			defer fp.mu.Unlock()

			fp.firstAckCh = nil
			fp.initialErrCh = nil
			fp.closed = true
			fp.ackSub.Unsubscribe()
			fp.ackSub = nil
			return nil, fmt.Errorf("batch message %d ack timeout", fp.sequence)
		}
	}

	// other than first message, we just publish and track pending acks.
	// if we exceed max outstanding acks, we stall until we get a flow ack.
	seq := fp.sequence
	if err := fp.js.Conn().PublishMsg(msg); err != nil {
		fp.mu.Unlock()
		return nil, fmt.Errorf("batch message %d publish failed: %w", seq, err)
	}

	if fp.waitForAck() {
		stallCh := fp.stallCh
		fp.mu.Unlock()
		if err := fp.waitForStall(stallCh); err != nil {
			fp.mu.Lock()
			fp.closed = true
			fp.mu.Unlock()
			return nil, fmt.Errorf("batch message %d %w; current ack sequence: %d", fp.sequence, err, fp.ackSequence)
		}
		fp.mu.Lock()
	}

	ackSeq := fp.ackSequence
	fp.mu.Unlock()

	return &FastPubAck{
		BatchSequence: seq,
		AckSequence:   ackSeq,
	}, nil
}

// Lock should be held.
func (fp *fastPublisher) waitForAck() bool {
	return fp.ackSequence+uint64(fp.flow)*uint64(fp.opts.maxOutstandingAcks) <= fp.sequence
}

// applyBatchMsgOpts processes batch message options and sets the appropriate
// headers on the message. Only allocates a header map when opts require it.
func applyBatchMsgOpts(msg *nats.Msg, opts []BatchMsgOpt) error {
	if len(opts) == 0 {
		return nil
	}
	var o batchMsgOpts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}
	if o.ttl == 0 && o.stream == "" && o.lastSubjectSeq == nil && o.lastSeq == nil {
		return nil
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
	if o.lastSubject != "" {
		msg.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, o.lastSubject)
		msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	} else if o.lastSubjectSeq != nil {
		msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	}
	if o.lastSeq != nil {
		msg.Header.Set(jetstream.ExpectedLastSeqHeader, strconv.FormatUint(*o.lastSeq, 10))
	}
	return nil
}

func (fp *fastPublisher) Commit(ctx context.Context, subject string, data []byte, opts ...BatchMsgOpt) (*BatchAck, error) {
	return fp.CommitMsg(ctx, &nats.Msg{
		Subject: subject,
		Data:    data,
	}, opts...)
}

func (fp *fastPublisher) CommitMsg(ctx context.Context, msg *nats.Msg, opts ...BatchMsgOpt) (*BatchAck, error) {
	fp.mu.Lock()

	if fp.closed {
		fp.mu.Unlock()
		return nil, ErrBatchClosed
	}

	if err := applyBatchMsgOpts(msg, opts); err != nil {
		fp.mu.Unlock()
		return nil, err
	}

	fp.mu.Unlock()
	return fp.commit(ctx, msg, false)
}

func (fp *fastPublisher) commit(ctx context.Context, msg *nats.Msg, eob bool) (*BatchAck, error) {
	fp.mu.Lock()
	fp.sequence++
	operation := fastBatchCommitMsg
	if eob {
		operation = fastBatchCommitEOB
	}
	msg.Reply = fp.buildReplySubject(fp.sequence, operation)

	if fp.commitCh == nil {
		fp.commitCh = make(chan *batchAckResponse, 1)
	}
	if fp.ackSub == nil {
		ackSub, err := fp.js.Conn().Subscribe(fmt.Sprintf("%s.>", fp.ackInboxPrefix), fp.ackMsgHandler)
		if err != nil {
			fp.mu.Unlock()
			return nil, err
		}
		fp.ackSub = ackSub
	}
	if err := fp.js.Conn().PublishMsg(msg); err != nil {
		fp.mu.Unlock()
		return nil, fmt.Errorf("batch commit failed: %w", err)
	}

	// Release lock before waiting for commit response - handler needs lock to send to commitCh
	fp.mu.Unlock()

	deadline, ping, pingInterval := fp.newPingTimers()
	defer deadline.Stop()
	defer ping.Stop()

	var batchAck *BatchAck
	var commitErr error
	for {
		select {
		case commitResp := <-fp.commitCh:
			if commitResp.Error != nil {
				commitErr = commitResp.Error
			} else if commitResp.BatchAck == nil || commitResp.Stream == "" {
				commitErr = ErrInvalidBatchAck
			} else {
				batchAck = commitResp.BatchAck
			}
		case <-ping.C:
			if err := fp.sendPing(); err != nil {
				commitErr = fmt.Errorf("ping failed: %w", err)
				break
			}
			ping.Reset(pingInterval)
			continue
		case <-deadline.C:
			commitErr = errors.New("ack timeout")
		case <-ctx.Done():
			fp.mu.Lock()
			fp.closed = true
			if fp.ackSub != nil {
				fp.ackSub.Unsubscribe()
				fp.ackSub = nil
			}
			fp.mu.Unlock()
			return nil, ctx.Err()
		}
		break
	}

	// Reacquire lock to safely modify shared state
	fp.mu.Lock()
	fp.closed = true
	if fp.ackSub != nil {
		fp.ackSub.Unsubscribe()
		fp.ackSub = nil
	}
	fp.mu.Unlock()

	return batchAck, commitErr
}

func (fp *fastPublisher) Close(ctx context.Context) (*BatchAck, error) {
	fp.mu.Lock()

	if fp.sequence == 0 {
		fp.mu.Unlock()
		return nil, ErrEmptyBatch
	}
	if fp.closed {
		fp.mu.Unlock()
		return nil, ErrBatchClosed
	}
	fp.mu.Unlock()
	return fp.commit(ctx, nats.NewMsg(fp.batchSubject), true)
}

func (fp *fastPublisher) IsClosed() bool {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.closed
}

func (fp *fastPublisher) ackMsgHandler(msg *nats.Msg) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	var commitAck *batchAckResponse
	var flowAck *batchFlowAck
	var flowErr *batchFlowErr

	switch {
	case bytes.Contains(msg.Data, []byte(`"type":"gap"`)):
		var gapErr *batchFlowGap
		if err := json.Unmarshal(msg.Data, &gapErr); err != nil {
			if fp.errHandler != nil {
				fp.errHandler(err)
			}
			return
		}
		if fp.errHandler != nil {
			fp.errHandler(fmt.Errorf("%w: expected last sequence %d; current sequence %d", ErrFastBatchGapDetected, gapErr.ExpectedLastSequence, gapErr.CurrentSequence))
		}
		return
	case bytes.Contains(msg.Data, []byte(`"type":"ack"`)):
		if err := json.Unmarshal(msg.Data, &flowAck); err != nil {
			if fp.errHandler != nil {
				fp.errHandler(err)
			}
			return
		}

		if flowAck != nil {
			if fp.flow != flowAck.Messages {
				fp.flow = flowAck.Messages
			}
			fp.ackSequence = flowAck.Sequence
			// Handle first message ack specially - firstAckCh is only set for first message
			if fp.firstAckCh != nil {
				fp.firstAckCh <- flowAck
				close(fp.firstAckCh)
				fp.firstAckCh = nil
				return
			}

			// Handle flow ack. We want to let the publisher know if we are no longer stalled
			// and can continue publishing.
			select {
			case fp.stallCh <- struct{}{}:
			default:
			}
		}
	case bytes.Contains(msg.Data, []byte(`"type":"err"`)):
		if err := json.Unmarshal(msg.Data, &flowErr); err != nil {
			if fp.errHandler != nil {
				fp.errHandler(err)
			}
			return
		}
		if fp.errHandler != nil {
			fp.errHandler(fmt.Errorf("error processing batch at sequence %d: %w", flowErr.Sequence, flowErr.Error))
		}
		return
	default:
		// no type hint, thus unmarshal as commit ack
		// this always means end of batch
		if err := json.Unmarshal(msg.Data, &commitAck); err != nil {
			if fp.errHandler != nil {
				fp.errHandler(err)
			}
			return
		}
		fp.closed = true
		if fp.commitCh != nil {
			fp.commitCh <- commitAck
		} else if commitAck != nil && commitAck.Error != nil && fp.errHandler != nil {
			fp.errHandler(commitAck.Error)
		}
		if fp.ackSub != nil {
			fp.ackSub.Unsubscribe()
			fp.ackSub = nil
		}
		return
	}
}
