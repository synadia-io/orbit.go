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
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type (
	// ChannelPublisher publishes messages asynchronously and delivers
	// acks/errors on a single channel.
	ChannelPublisher interface {
		// Publish publishes a message asynchronously.
		Publish(subject string, payload []byte, opts ...PublishOpt) error

		// PublishMsg publishes a nats.Msg asynchronously.
		PublishMsg(msg *nats.Msg, opts ...PublishOpt) error

		// Results returns the channel where acks/errors are delivered.
		Results() <-chan PubAckResult

		// Close stops the publisher and closes the results channel.
		// It processes any pending acks received by the client before closing.
		Close()

		// Pending returns number of publishes waiting for ack.
		Pending() int
	}

	// PubAckResult contains the result of an async publish operation.
	PubAckResult struct {
		// Ack is the publish acknowledgment from the server.
		Ack *jetstream.PubAck
		// Err is any error that occurred during publish.
		Err error
		// Msg is the original message that was published.
		Msg *nats.Msg
	}

	channelPublisher struct {
		js          jetstream.JetStream
		results     chan PubAckResult
		sub         *nats.Subscription
		replyPrefix string
		mu          sync.Mutex
		pending     map[string]*pendingPub
		closed      bool
		rr          *rand.Rand
		stallCh     chan struct{}
	}

	pendingPub struct {
		msg     *nats.Msg
		timeout *time.Timer
	}

	channelPublisherOpts struct {
		channelBuffer int
	}

	// ChannelPublisherOpt is a functional option for configuring a ChannelPublisher.
	ChannelPublisherOpt func(*channelPublisherOpts) error

	// PublishOpt is a functional option for configuring a publish operation.
	PublishOpt func(*publishOpts) error

	publishOpts struct {
		msgID               string
		expectedStream      string
		expectedLastSeq     *uint64
		expectedLastSubjSeq *uint64
		expectedLastSubject string
		expectedLastMsgID   string
		ttl                 time.Duration
		stallWait           time.Duration
	}
)

const (
	defaultChannelBuffer = 1000
	defaultStallWait     = 200 * time.Millisecond
	aReplyTokensize      = 6
	rdigits              = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	base                 = 62
)

// NewChannelPublisher creates a new channel-based async publisher.
func NewChannelPublisher(js jetstream.JetStream, opts ...ChannelPublisherOpt) (ChannelPublisher, error) {
	pubOpts := channelPublisherOpts{
		channelBuffer: defaultChannelBuffer,
	}

	for _, opt := range opts {
		if err := opt(&pubOpts); err != nil {
			return nil, err
		}
	}

	cp := &channelPublisher{
		js:          js,
		results:     make(chan PubAckResult, pubOpts.channelBuffer),
		pending:     make(map[string]*pendingPub),
		replyPrefix: js.Conn().NewRespInbox() + ".",
		rr:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// Create single subscription for all async acks
	sub, err := js.Conn().Subscribe(cp.replyPrefix+"*", cp.handleAsyncReply)
	if err != nil {
		return nil, fmt.Errorf("failed to create reply subscription: %w", err)
	}
	cp.sub = sub

	return cp, nil
}

// Publish publishes a message asynchronously.
func (cp *channelPublisher) Publish(subject string, payload []byte, opts ...PublishOpt) error {
	return cp.PublishMsg(&nats.Msg{Subject: subject, Data: payload}, opts...)
}

// PublishMsg publishes a nats.Msg asynchronously.
func (cp *channelPublisher) PublishMsg(msg *nats.Msg, opts ...PublishOpt) error {
	if msg.Reply != "" {
		return ErrChannelPublisherReplySubjectSet
	}
	o := publishOpts{
		stallWait: defaultStallWait,
	}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}

	cp.mu.Lock()
	if cp.closed {
		cp.mu.Unlock()
		return ErrPublisherClosed
	}

	jsOpts := cp.js.Options()
	maxPending := jsOpts.PublisherOpts.MaxAckPending

	if maxPending > 0 && len(cp.pending) >= maxPending {
		// Create stall channel if needed
		if cp.stallCh == nil {
			cp.stallCh = make(chan struct{})
		}
		stallCh := cp.stallCh

		cp.mu.Unlock()

		// Wait for pending to drop below max or timeout
		select {
		case <-stallCh:
			// Channel closed - pending dropped below max, continue with publish
			cp.mu.Lock()
			if cp.closed {
				return ErrPublisherClosed
			}
		case <-time.After(o.stallWait):
			return ErrTooManyStalledMsgs
		}
	}

	var sb strings.Builder
	sb.WriteString(cp.replyPrefix)
	for {
		rn := cp.rr.Int63()
		var b [aReplyTokensize]byte
		for i, l := 0, rn; i < len(b); i++ {
			b[i] = rdigits[l%base]
			l /= base
		}
		if _, ok := cp.pending[string(b[:])]; ok {
			continue
		}
		sb.Write(b[:])
		break

	}
	id := sb.String()[len(cp.replyPrefix):]

	// Apply publish options to message headers
	if err := applyPublishOptsHeaders(msg, &o); err != nil {
		cp.mu.Unlock()
		return err
	}

	pending := &pendingPub{msg: msg}
	cp.pending[id] = pending

	// Set reply subject for ack
	msg.Reply = sb.String()

	cp.mu.Unlock()

	if err := cp.js.Conn().PublishMsg(msg); err != nil {
		cp.mu.Lock()
		delete(cp.pending, id)
		cp.mu.Unlock()
		return err
	}

	// Set up ack timeout if configured
	if jsOpts.PublisherOpts.AckTimeout > 0 {
		pending.timeout = time.AfterFunc(jsOpts.PublisherOpts.AckTimeout, func() {
			cp.mu.Lock()
			defer cp.mu.Unlock()

			// Check if ack already received
			if _, ok := cp.pending[id]; !ok {
				return
			}

			// Timeout occurred - remove from pending
			delete(cp.pending, id)

			result := PubAckResult{
				Msg: msg,
				Err: ErrAckTimeout,
			}
			select {
			case cp.results <- result:
			default:
				// Channel full or closed
			}
		})
	}

	return nil
}

// applyPublishOptsHeaders applies publish options to message headers.
func applyPublishOptsHeaders(msg *nats.Msg, o *publishOpts) error {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	if o.msgID != "" {
		msg.Header.Set(jetstream.MsgIDHeader, o.msgID)
	}
	if o.expectedStream != "" {
		msg.Header.Set(jetstream.ExpectedStreamHeader, o.expectedStream)
	}
	if o.expectedLastSeq != nil {
		msg.Header.Set(jetstream.ExpectedLastSeqHeader, strconv.FormatUint(*o.expectedLastSeq, 10))
	}
	if o.expectedLastSubjSeq != nil {
		msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.expectedLastSubjSeq, 10))
	}
	if o.expectedLastSubject != "" {
		msg.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, o.expectedLastSubject)
	}
	if o.expectedLastMsgID != "" {
		msg.Header.Set(jetstream.ExpectedLastMsgIDHeader, o.expectedLastMsgID)
	}
	if o.ttl > 0 {
		msg.Header.Set(jetstream.MsgTTLHeader, o.ttl.String())
	}

	return nil
}

// handleAsyncReply processes incoming ack messages from the subscription.
func (cp *channelPublisher) handleAsyncReply(m *nats.Msg) {
	// Extract token from reply subject
	token := strings.TrimPrefix(m.Subject, cp.replyPrefix)

	// Look up pending publish
	cp.mu.Lock()
	pending, found := cp.pending[token]
	if !found {
		cp.mu.Unlock()
		return
	}
	delete(cp.pending, token)

	// Stop timeout timer if it exists
	if pending.timeout != nil {
		pending.timeout.Stop()
	}

	// Close stall channel if pending dropped below max
	jsOpts := cp.js.Options()
	maxPending := jsOpts.PublisherOpts.MaxAckPending
	if cp.stallCh != nil && maxPending > 0 && len(cp.pending) < maxPending {
		close(cp.stallCh)
		cp.stallCh = nil
	}

	cp.mu.Unlock()

	result := PubAckResult{Msg: pending.msg}

	// Parse ack response
	if len(m.Data) > 0 {
		var ackResp struct {
			jetstream.PubAck
			Error *jetstream.APIError `json:"error,omitempty"`
		}

		if err := json.Unmarshal(m.Data, &ackResp); err != nil {
			result.Err = fmt.Errorf("failed to parse ack: %w", err)
		} else if ackResp.Error != nil {
			result.Err = ackResp.Error
		} else {
			result.Ack = &ackResp.PubAck
		}
	} else {
		result.Err = fmt.Errorf("empty ack response")
	}

	select {
	case cp.results <- result:
	default:
		// Channel is full or closed, drop result
	}
}

// Results returns the channel where acks/errors are delivered.
func (cp *channelPublisher) Results() <-chan PubAckResult {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.results
}

// Close stops the publisher and closes the results channel.
func (cp *channelPublisher) Close() {
	cp.mu.Lock()
	if cp.closed {
		cp.mu.Unlock()
		return
	}
	cp.closed = true

	// Close stall channel to unblock any waiting publishers
	if cp.stallCh != nil {
		close(cp.stallCh)
		cp.stallCh = nil
	}
	cp.mu.Unlock()

	// drain the subscription to make sure all incoming acks are processed
	if cp.sub != nil {
		cp.sub.Drain()
	}

	cp.mu.Lock()
	for token, pending := range cp.pending {
		if pending.timeout != nil {
			pending.timeout.Stop()
		}

		result := PubAckResult{
			Msg: pending.msg,
			Err: ErrPublisherClosed,
		}
		select {
		case cp.results <- result:
		default:
		}
		delete(cp.pending, token)
	}
	cp.mu.Unlock()

	// Close the results channel
	close(cp.results)
}

// Pending returns number of publishes waiting for ack.
func (cp *channelPublisher) Pending() int {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return len(cp.pending)
}
