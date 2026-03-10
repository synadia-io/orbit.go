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
	FastPublisher interface {
		// Add publishes a message to the batch with the given subject and data.
		// It is an IO operation and the message will be published immediately
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

		// Close closes the batch, signalling the server that no more messages will be added.
		// It sends an EOB commit to the server, without adding a message.
		Close(ctx context.Context) (*BatchAck, error)

		// IsClosed returns true if the batch has been committed or discarded.
		IsClosed() bool
	}
	// FastPublishFlowControl configures flow control for fast batch publishing.
	FastPublishFlowControl struct {
		// Flow is the initial flow control value (ack frequency by message count).
		// Server may adjust this dynamically via BatchFlowAck responses.
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

	BatchFlowAck struct {
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

	// BatchFlowGap is used for reporting gaps when fast batch publishing into a stream.
	// This message is purely informational and could technically be lost without the client receiving it.
	BatchFlowGap struct {
		// Type: "gap"
		Type string `json:"type"`
		// ExpectedLastSequence is the sequence expected to be received next.
		// Messages starting from ExpectedLastSequence up to (but not including) CurrentSequence were lost.
		ExpectedLastSequence uint64 `json:"last_seq"`
		// CurrentSequence is the sequence of the message that just came in and detected the gap.
		CurrentSequence uint64 `json:"seq"`
	}

	// BatchFlowErr is used for reporting errors with "gap: ok" when fast batch publishing into a stream.
	// This message is purely informational and could technically be lost without the client receiving it.
	BatchFlowErr struct {
		// Type: "err"
		Type string `json:"type"`
		// Sequence is the sequence of the message that triggered the error.
		// There are no (relative) guarantees whatsoever about whether the messages up to this sequence were persisted.
		Sequence uint64 `json:"seq"`
		// Error is used for "gap:ok" to return the error for the Sequence.
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
		ackSub         *nats.Subscription
		sequence       uint64
		ackSequence    uint64
		closed         bool
		opts           fastPublisherOpts
		stallCh        chan struct{}
		commitCh       chan *batchAckResponse
		firstAckCh     chan *BatchFlowAck
		initialErrCh   chan error
		errHandler     FastPublishErrHandler
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
	// TODO: decide on defaults
	defaultFastFlow           = 100
	defaultMaxOutstandingAcks = 2
)

const (
	FastBatchStart = iota
	FastBatchAddMsg
	FastBatchCommitMsg
	FastBatchCommitEOB
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

	return &fastPublisher{
		js:             js,
		ackInboxPrefix: js.Conn().NewInbox(),
		flow:           pubOpts.flow,
		opts:           pubOpts,
		errHandler:     pubOpts.errHandler,
	}, nil
}

// TODO: Should we use unique option type?
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

	if msg.Header == nil {
		msg.Header = nats.Header{}
	}

	// Process batch message options
	o := batchMsgOpts{}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			fp.mu.Unlock()
			return nil, err
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

	fp.sequence++
	gap := "fail"
	if fp.opts.continueOnGap {
		gap = "ok"
	}
	operation := FastBatchAddMsg
	if fp.sequence == 1 {
		operation = FastBatchStart
	}
	msg.Reply = fmt.Sprintf("%s.%d.%s.%d.%d.$FI", fp.ackInboxPrefix, fp.flow, gap, fp.sequence, operation)

	// Create subscription and handle first message specially
	if fp.sequence == 1 {
		ackSub, err := fp.js.Conn().Subscribe(fmt.Sprintf("%s.>", fp.ackInboxPrefix), fp.ackMsgHandler)
		if err != nil {
			fp.mu.Unlock()
			return nil, err
		}
		fp.ackSub = ackSub

		// Create channel to receive first ack
		firstAckCh := make(chan *BatchFlowAck, 1)
		fp.firstAckCh = firstAckCh
		initialErrCh := make(chan error, 1)
		fp.initialErrCh = initialErrCh

		// Publish with reply inbox already set (without holding lock)
		if err := fp.js.Conn().PublishMsg(msg); err != nil {
			return nil, fmt.Errorf("batch message %d publish failed: %w", fp.sequence, err)
		}

		// Release lock to enable ack handler to run
		fp.mu.Unlock()
		select {
		case firstAck := <-firstAckCh:
			// Re-acquire lock to update state
			fp.mu.Lock()
			defer fp.mu.Unlock()

			fp.flow = firstAck.Messages
			fp.batchSubject = msg.Subject
			return &FastPubAck{
				BatchSequence: fp.sequence,
				AckSequence:   firstAck.Sequence,
			}, nil

		case err := <-fp.initialErrCh:
			return nil, fmt.Errorf("batch message %d ack error: %w", fp.sequence, err)
		case <-time.After(fp.opts.ackTimeout):
			// Re-acquire lock to mark closed
			fp.mu.Lock()
			defer fp.mu.Unlock()

			fp.closed = true
			return nil, fmt.Errorf("batch message %d ack timeout", fp.sequence)
		}
	}

	// other than first message, we just publish and track pending acks.
	// if we exceed max outstanding acks, we stall until we get a flow ack.
	defer fp.mu.Unlock()

	waitForAck := fp.ackSequence+uint64(fp.flow)*uint64(fp.opts.maxOutstandingAcks) < fp.sequence
	if waitForAck {
		var stallCh chan struct{}
		if fp.stallCh == nil {
			fp.stallCh = make(chan struct{}, 1)
			stallCh = fp.stallCh
		}
		fp.mu.Unlock()
		select {
		case <-stallCh:
		case <-time.After(fp.opts.ackTimeout):
			fp.mu.Lock()
			fp.closed = true
			return nil, fmt.Errorf("batch message %d ack timeout; current sequence: %d", fp.sequence, fp.ackSequence)
		}
		fp.mu.Lock()
	}
	if err := fp.js.Conn().PublishMsg(msg); err != nil {
		return nil, fmt.Errorf("batch message %d publish failed: %w", fp.sequence, err)
	}

	return &FastPubAck{
		BatchSequence: fp.sequence,
		AckSequence:   fp.ackSequence,
	}, nil
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

	if msg.Header == nil {
		msg.Header = nats.Header{}
	}

	// Process batch message options
	o := batchMsgOpts{}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			fp.mu.Unlock()
			return nil, err
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

	fp.mu.Unlock()
	return fp.commit(ctx, msg, false)
}

func (fp *fastPublisher) commit(ctx context.Context, msg *nats.Msg, eob bool) (*BatchAck, error) {
	fp.mu.Lock()
	fp.sequence++
	gap := "fail"
	if fp.opts.continueOnGap {
		gap = "ok"
	}
	operation := FastBatchCommitMsg
	if eob {
		operation = FastBatchCommitEOB
	}
	msg.Reply = fmt.Sprintf("%s.%d.%s.%d.%d.$FI", fp.ackInboxPrefix, fp.flow, gap, fp.sequence, operation)

	if fp.commitCh == nil {
		fp.commitCh = make(chan *batchAckResponse, 1)
	}
	if fp.ackSub == nil {
		ackSub, err := fp.js.Conn().Subscribe(fp.ackInboxPrefix, fp.ackMsgHandler)
		if err != nil {
			fp.mu.Unlock()
			return nil, err
		}
		fp.ackSub = ackSub
	}
	waitForAck := fp.ackSequence+uint64(fp.flow)*uint64(fp.opts.maxOutstandingAcks) < fp.sequence
	if waitForAck {
		var stallCh chan struct{}
		if fp.stallCh == nil {
			fp.stallCh = make(chan struct{}, 1)
			stallCh = fp.stallCh
		}
		fp.mu.Unlock()
		select {
		case <-stallCh:
		case <-time.After(fp.opts.ackTimeout):
			fp.mu.Lock()
			fp.closed = true
			fp.mu.Unlock()
			return nil, fmt.Errorf("batch commit timeout waiting for acks")
		}
		fp.mu.Lock()
	}
	if err := fp.js.Conn().PublishMsg(msg); err != nil {
		fp.mu.Unlock()
		return nil, fmt.Errorf("batch commit failed: %w", err)
	}

	// Release lock before waiting for commit response - handler needs lock to send to commitCh
	fp.mu.Unlock()

	// wait for commit ack or context cancel
	var batchAck *BatchAck
	var commitErr error
	select {
	case commitResp := <-fp.commitCh:
		if commitResp.Error != nil {
			commitErr = commitResp.Error
			break
		}
		if commitResp.BatchAck == nil || commitResp.Stream == "" {
			commitErr = ErrInvalidBatchAck
			break
		}
		batchAck = commitResp.BatchAck
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
		return nil, errors.New("no messages in batch")
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
	var respErr error
	var flowAck *BatchFlowAck
	var flowErr *BatchFlowErr

	switch {
	case bytes.Contains(msg.Data, []byte(`"type":"gap"`)):
		var gapErr *BatchFlowGap
		if err := json.Unmarshal(msg.Data, &gapErr); err != nil {
			respErr = err
		} else {
			if fp.errHandler != nil {
				fp.errHandler(fmt.Errorf("%w: expected last sequence %d; current sequence %d", ErrFastBatchGapDetected, gapErr.ExpectedLastSequence, gapErr.CurrentSequence))
			}
			return
		}
	case bytes.Contains(msg.Data, []byte(`"type":"ack"`)):
		if err := json.Unmarshal(msg.Data, &flowAck); err != nil {
			respErr = err
		}
		if respErr != nil && fp.errHandler != nil {
			fp.errHandler(respErr)
			return
		}

		if flowAck != nil {
			fp.flow = flowAck.Messages
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
			if fp.stallCh != nil {
				// Unblock publisher if we were stalled and are now below the max outstanding acks
				close(fp.stallCh)
				fp.stallCh = nil
			}
		}
	case bytes.Contains(msg.Data, []byte(`"type":"err"`)):
		if err := json.Unmarshal(msg.Data, &flowErr); err != nil {
			respErr = err
		} else {
			if fp.errHandler != nil {
				fp.errHandler(fmt.Errorf("Error processing batch at sequence %d: %w", flowErr.Sequence, flowErr.Error))
			}
			return
		}
	default:
		// no type hint, thus unmarshal as commit ack
		// this always means end of batch
		if err := json.Unmarshal(msg.Data, &commitAck); err != nil {
			respErr = err
		}
		fp.closed = true
		if fp.commitCh != nil {
			fp.commitCh <- commitAck
		}
		// only invoke callback if there is an error and we're NOT during commit (as commit would surface the error)
		if commitAck.Error != nil && fp.commitCh != nil {
			if fp.errHandler != nil {
				fp.errHandler(commitAck.Error)
			}
			return
		}
		if fp.ackSub != nil {
			fp.ackSub.Unsubscribe()
			fp.ackSub = nil
		}
		return
	}
}
