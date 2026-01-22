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

// ChannelPublisher options

// WithChannelBuffer sets the result channel buffer size.
// Default is 1000.
func WithChannelBuffer(size int) ChannelPublisherOpt {
	return func(opts *channelPublisherOpts) error {
		if size <= 0 {
			return fmt.Errorf("%w: channel buffer size must be greater than 0", ErrInvalidOption)
		}
		opts.channelBuffer = size
		return nil
	}
}

// Publish options

// WithMsgID sets the message ID for deduplication.
func WithMsgID(id string) PublishOpt {
	return func(opts *publishOpts) error {
		opts.msgID = id
		return nil
	}
}

// WithExpectStream sets the expected stream the message should be published to.
func WithExpectStream(stream string) PublishOpt {
	return func(opts *publishOpts) error {
		opts.expectedStream = stream
		return nil
	}
}

// WithExpectLastSequence sets the expected sequence number of the last message in the stream.
func WithExpectLastSequence(seq uint64) PublishOpt {
	return func(opts *publishOpts) error {
		opts.expectedLastSeq = &seq
		return nil
	}
}

// WithExpectLastSequencePerSubject sets the expected sequence number of the last message
// on the subject the message is published to.
func WithExpectLastSequencePerSubject(seq uint64) PublishOpt {
	return func(opts *publishOpts) error {
		opts.expectedLastSubjSeq = &seq
		return nil
	}
}

// WithExpectLastSequenceForSubject sets the sequence and subject for which the
// last sequence number should be checked.
func WithExpectLastSequenceForSubject(seq uint64, subject string) PublishOpt {
	return func(opts *publishOpts) error {
		if subject == "" {
			return fmt.Errorf("%w: subject cannot be empty", ErrInvalidOption)
		}
		opts.expectedLastSubjSeq = &seq
		opts.expectedLastSubject = subject
		return nil
	}
}

// WithExpectLastMsgID sets the expected last message ID.
func WithExpectLastMsgID(id string) PublishOpt {
	return func(opts *publishOpts) error {
		opts.expectedLastMsgID = id
		return nil
	}
}

// WithMsgTTL sets per-message TTL.
func WithMsgTTL(ttl time.Duration) PublishOpt {
	return func(opts *publishOpts) error {
		opts.ttl = ttl
		return nil
	}
}

// WithStallWait sets the max wait when the producer becomes stalled.
// If a publish call is blocked for this long due to max pending acks,
// ErrTooManyStalledMsgs is returned.
func WithStallWait(wait time.Duration) PublishOpt {
	return func(opts *publishOpts) error {
		opts.stallWait = wait
		return nil
	}
}
