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

import "fmt"

// configureFastPublisher implements the FastPublisherOpt interface for FastPublishFlowControl,
// allowing the flow control struct to be used as an option.
func (fc FastPublishFlowControl) configureFastPublisher(opts *fastPublisherOpts) error {
	// Validate non-negative values
	if fc.Flow < 0 {
		return fmt.Errorf("%w: flow must be non-negative", ErrInvalidOption)
	}
	if fc.MaxOutstandingAcks < 0 {
		return fmt.Errorf("%w: max outstanding acks must be non-negative", ErrInvalidOption)
	}
	if fc.AckTimeout < 0 {
		return fmt.Errorf("%w: ack timeout must be non-negative", ErrInvalidOption)
	}

	if fc.Flow > 0 {
		opts.flow = fc.Flow
	}
	if fc.MaxOutstandingAcks > 0 {
		opts.maxOutstandingAcks = fc.MaxOutstandingAcks
	}
	if fc.AckTimeout > 0 {
		opts.ackTimeout = fc.AckTimeout
	}
	return nil
}

type fastPublisherOptFunc func(*fastPublisherOpts) error

func (f fastPublisherOptFunc) configureFastPublisher(opts *fastPublisherOpts) error {
	return f(opts)
}

// WithFastPublisherContinueOnGap sets whether the batch should continue on gap detection.
// When false (default), server will abandon the batch upon detecting a gap.
// When true, the batch will continue but the error handler will be notified.
func WithFastPublisherContinueOnGap(continueOnGap bool) FastPublisherOpt {
	return fastPublisherOptFunc(func(opts *fastPublisherOpts) error {
		opts.continueOnGap = continueOnGap
		return nil
	})
}

// WithFastPublisherErrorHandler sets the error handler for async errors.
// The handler is called for flow ack errors, gap detection, and other async errors.
// Note: The handler is called while holding an internal lock, so it should be fast
// and non-blocking to avoid impacting performance.
func WithFastPublisherErrorHandler(handler FastPublishErrHandler) FastPublisherOpt {
	return fastPublisherOptFunc(func(opts *fastPublisherOpts) error {
		opts.errHandler = handler
		return nil
	})
}
