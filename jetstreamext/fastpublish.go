package jetstreamext

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
		// All messages persisted so far will remain in the stream.
		// After calling Discard, the batch is closed and no further
		// messages can be added.
		Discard() error

		// Size returns the number of messages added to the batch so far.
		Size() int

		// IsClosed returns true if the batch has been committed or discarded.
		IsClosed() bool
	}
	// FastPublishFlowControl configures flow control for fast batch publishing.
	FastPublishFlowControl struct {
		// Flow is the initial flow control value (ack frequency by message count).
		// Server may adjust this dynamically via BatchFlowAck responses.
		// Default: 100
		Flow int

		// MaxOutstandingAcks limits how many unacknowledged messages before stalling.
		// When this limit is reached, Add/AddMsg will block until acks are received.
		// Default: 10
		MaxOutstandingAcks int

		// AckTimeout is the timeout for waiting on acks.
		// Default: JetStream context default timeout
		AckTimeout time.Duration
	}

	BatchFlowAck struct {
		// LastSequence is the previously highest sequence seen, this is set when a gap is detected
		LastSequence int `json:"last_seq,omitempty"`
		// CurrentSequence is the sequence of the message that triggered the ack
		CurrentSequence int `json:"seq,omitempty"`
		// AckMessages indicates the active per-message frequency of Flow Acks
		AckMessages int `json:"messages,omitempty"`
		// AckBytes indicates the active per-bytes frequency of Flow Acks in unit of bytes
		AckBytes int64 `json:"bytes,omitempty"`
	}

	batchFlowAckResponse struct {
		*BatchFlowAck
		apiResponse
	}

	// FastPublisherOpt is a functional option for configuring a FastPublisher.
	FastPublisherOpt interface {
		configureFastPublisher(*fastPublisherOpts) error
	}

	fastPublisher struct {
		js          jetstream.JetStream
		batchID     string
		flow        int
		ackInbox    string
		ackSub      *nats.Subscription
		sequence    int
		closed      bool
		opts        fastPublisherOpts
		stallCh     chan struct{}
		commitCh    chan *batchAckResponse
		firstAckCh  chan *batchFlowAckResponse
		errHandler  FastPublishErrHandler
		pendingAcks map[int]struct{}
		mu          sync.Mutex
	}

	FastPublishErrHandler func(error)

	fastPublisherOpts struct {
		continueOnGap      bool
		flow               int
		maxOutstandingAcks int
		ackTimeout         time.Duration
		errHandler         FastPublishErrHandler
	}
)

const (
	// FastBatchIDHeader is the header used to identify the batch ID in messages.
	FastBatchIDHeader = "Nats-Fast-Batch-Id"

	// FastBatchFlowHeader is the header used to identify the flow control settings in messages.
	FastBatchFlowHeader = "Nats-Flow"

	// FastBatchGapHeader is the header used to indicate if the server should abandon the batch upon detecting a gap.
	FastBatchGapHeader = "Nats-Batch-Gap"

	// TODO: decide on defaults
	defaultFastFlow           = 100
	defaultMaxOutstandingAcks = 10
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
		js:          js,
		batchID:     nuid.Next(),
		ackInbox:    js.Conn().NewInbox(),
		flow:        pubOpts.flow,
		opts:        pubOpts,
		errHandler:  pubOpts.errHandler,
		commitCh:    make(chan *batchAckResponse, 1),
		pendingAcks: make(map[int]struct{}),
	}, nil
}

// TODO: Should we use unique option type?
func (fp *fastPublisher) Add(subject string, data []byte, opts ...BatchMsgOpt) error {
	return fp.AddMsg(&nats.Msg{
		Subject: subject,
		Data:    data,
	}, opts...)
}

func (fp *fastPublisher) AddMsg(msg *nats.Msg, opts ...BatchMsgOpt) error {
	fp.mu.Lock()

	if fp.closed {
		fp.mu.Unlock()
		return ErrBatchClosed
	}

	if msg.Header == nil {
		msg.Header = nats.Header{}
	}

	// Process batch message options
	o := batchMsgOpts{}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			fp.mu.Unlock()
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

	fp.sequence++
	msg.Header.Set(FastBatchIDHeader, fp.batchID)
	msg.Header.Set(FastBatchFlowHeader, strconv.Itoa(fp.flow))
	msg.Header.Set(BatchSeqHeader, strconv.Itoa(fp.sequence))
	if fp.opts.continueOnGap {
		msg.Header.Set(FastBatchGapHeader, "ok")
	}
	msg.Reply = fp.ackInbox

	// Create subscription and handle first message specially
	if fp.sequence == 1 {
		ackSub, err := fp.js.Conn().Subscribe(fp.ackInbox, fp.ackMsgHandler)
		if err != nil {
			fp.mu.Unlock()
			return err
		}
		fp.ackSub = ackSub

		// Create channel to receive first ack
		firstAckCh := make(chan *batchFlowAckResponse, 1)
		fp.firstAckCh = firstAckCh

		// Publish with reply inbox already set (without holding lock)
		if err := fp.js.Conn().PublishMsg(msg); err != nil {
			return fmt.Errorf("batch message %d publish failed: %w", fp.sequence, err)
		}

		// Release lock to enable ack handler to run
		fp.mu.Unlock()
		select {
		case firstAck := <-firstAckCh:
			// Re-acquire lock to update state
			fp.mu.Lock()
			defer fp.mu.Unlock()

			if firstAck.Error != nil {
				return fmt.Errorf("batch message %d ack error: %w", fp.sequence, firstAck.Error)
			}
			fp.flow = firstAck.AckMessages
			return nil

		case <-time.After(fp.opts.ackTimeout):
			// Re-acquire lock to mark closed
			fp.mu.Lock()
			defer fp.mu.Unlock()

			fp.closed = true
			return fmt.Errorf("batch message %d ack timeout", fp.sequence)
		}
	}

	// other than first message, we just publish and track pending acks.
	// if we exceed max outstanding acks, we stall until we get a flow ack.
	defer fp.mu.Unlock()

	if len(fp.pendingAcks) > fp.opts.maxOutstandingAcks {
		if fp.stallCh == nil {
			fp.stallCh = make(chan struct{}, 1)
		}
		fp.mu.Unlock()
		select {
		case <-fp.stallCh:
		case <-time.After(fp.opts.ackTimeout):
			fp.mu.Lock()
			fp.closed = true
			return fmt.Errorf("batch message %d ack timeout", fp.sequence)
		}
		fp.mu.Lock()
	}
	if err := fp.js.Conn().PublishMsg(msg); err != nil {
		return fmt.Errorf("batch message %d publish failed: %w", fp.sequence, err)
	}

	// Track pending acks for messages that will receive a flow ack
	// Message 1 always gets an ack, then every flow-th message
	if fp.sequence == 1 || fp.sequence%fp.flow == 0 {
		fp.pendingAcks[fp.sequence] = struct{}{}
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
	msg.Header.Set(FastBatchIDHeader, fp.batchID)
	msg.Header.Set(FastBatchFlowHeader, strconv.Itoa(fp.flow))
	msg.Header.Set(BatchSeqHeader, strconv.Itoa(fp.sequence))
	msg.Header.Set(BatchCommitHeader, "1")
	msg.Reply = fp.ackInbox

	if fp.ackSub == nil {
		ackSub, err := fp.js.Conn().Subscribe(fp.ackInbox, fp.ackMsgHandler)
		if err != nil {
			fp.mu.Unlock()
			return nil, err
		}
		fp.ackSub = ackSub
	}
	if len(fp.pendingAcks) >= fp.opts.maxOutstandingAcks {
		if fp.stallCh == nil {
			fp.stallCh = make(chan struct{}, 1)
		}
		fp.mu.Unlock()
		select {
		case <-fp.stallCh:
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
		if commitResp.BatchAck == nil || commitResp.Stream == "" ||
			commitResp.BatchID != fp.batchID {
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

func (fp *fastPublisher) Discard() error {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	if fp.closed {
		return ErrBatchClosed
	}
	fp.closed = true
	if fp.ackSub != nil {
		fp.ackSub.Unsubscribe()
		fp.ackSub = nil
	}
	return nil
}

func (fp *fastPublisher) Size() int {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.sequence
}

func (fp *fastPublisher) IsClosed() bool {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.closed
}

func (fp *fastPublisher) ackMsgHandler(msg *nats.Msg) {
	var commitAck *batchAckResponse
	var flowAck *batchFlowAckResponse
	var respErr error

	if bytes.Contains(msg.Data, []byte(`"batch":`)) {
		// Has "batch" field, unmarshal as commit ack
		if err := json.Unmarshal(msg.Data, &commitAck); err != nil {
			respErr = err
		}
	} else {
		// No "batch" field, unmarshal as flow ack
		if err := json.Unmarshal(msg.Data, &flowAck); err != nil {
			respErr = err
		}
	}

	fp.mu.Lock()
	defer fp.mu.Unlock()
	if respErr != nil && fp.errHandler != nil {
		fp.errHandler(respErr)
		return
	}

	if commitAck != nil {
		if commitAck.Error != nil {
			fp.sendErr(commitAck.Error)
			return
		}

		fp.commitCh <- commitAck
		fp.closed = true
		return
	}

	if flowAck != nil {
		if flowAck.Error != nil {
			fp.sendErr(flowAck.Error)
			return
		}
		fp.flow = flowAck.AckMessages
		// Handle first message ack specially - firstAckCh is only set for first message
		if fp.firstAckCh != nil {
			fp.firstAckCh <- flowAck
			close(fp.firstAckCh)
			fp.firstAckCh = nil
			return
		}

		// Clean up all pending acks up to and including the current sequence
		maps.DeleteFunc(fp.pendingAcks, func(k int, _ struct{}) bool {
			return k <= flowAck.CurrentSequence
		})

		if flowAck.LastSequence > 0 {
			if !fp.opts.continueOnGap {
				fp.sendErr(fmt.Errorf("%w: last sequence %d; current sequence %d", ErrFastBatchGapDetected, flowAck.LastSequence, flowAck.CurrentSequence))
				return
			}
			if fp.errHandler != nil {
				fp.errHandler(fmt.Errorf("%w: last sequence %d; current sequence %d", ErrFastBatchGapDetected, flowAck.LastSequence, flowAck.CurrentSequence))
			}
			return
		}
		// Handle flow ack. We want to let the publisher know if we are no longer stalled
		// and can continue publishing.
		if fp.stallCh != nil && len(fp.pendingAcks) < fp.opts.maxOutstandingAcks {
			// Unblock publisher if we were stalled and are now below the max outstanding acks
			close(fp.stallCh)
			fp.stallCh = nil
		}
	}
}

func (fp *fastPublisher) sendErr(err error) {
	if fp.errHandler != nil {
		fp.errHandler(err)
	}
	if !errors.Is(err, ErrFastBatchGapDetected) || !fp.opts.continueOnGap {
		fp.closed = true
		if fp.ackSub != nil {
			fp.ackSub.Unsubscribe()
			fp.ackSub = nil
		}
	}
}
