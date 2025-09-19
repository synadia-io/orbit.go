package jetstreamext

import (
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// Batch publish error codes
	JSErrCodeBatchPublishNotEnabled        jetstream.ErrorCode = 10174
	JSErrCodeBatchPublishMissingSeq        jetstream.ErrorCode = 10175
	JSErrCodeBatchPublishIncomplete        jetstream.ErrorCode = 10176
	JSErrCodeBatchPublishUnsupportedHeader jetstream.ErrorCode = 10177
	JSErrCodeBatchPublishExceedsLimit      jetstream.ErrorCode = 10199
)

var (
	// Batch publish errors

	// ErrBatchPublishNotEnabled is returned when batch publish is not enabled on the stream.
	ErrBatchPublishNotEnabled jetstream.JetStreamError = &jsError{apiErr: &jetstream.APIError{ErrorCode: JSErrCodeBatchPublishNotEnabled, Description: "batch publish not enabled on stream", Code: 400}}

	// ErrBatchPublishIncomplete is returned when batch publish is incomplete and was abandoned.
	ErrBatchPublishIncomplete jetstream.JetStreamError = &jsError{apiErr: &jetstream.APIError{ErrorCode: JSErrCodeBatchPublishIncomplete, Description: "batch publish is incomplete and was abandoned", Code: 400}}

	// ErrBatchPublishMissingSeq is returned when batch publish sequence is missing.
	ErrBatchPublishMissingSeq jetstream.JetStreamError = &jsError{apiErr: &jetstream.APIError{ErrorCode: JSErrCodeBatchPublishMissingSeq, Description: "batch publish sequence is missing", Code: 400}}

	// ErrBatchPublishExceedsLimit is returned when batch publish sequence exceeds server limit (default 1000).
	ErrBatchPublishExceedsLimit jetstream.JetStreamError = &jsError{apiErr: &jetstream.APIError{ErrorCode: JSErrCodeBatchPublishExceedsLimit, Description: "batch publish sequence exceeds server limit (default 1000)", Code: 400}}

	// ErrBatchPublishUnsupportedHeader is returned when batch publish uses unsupported headers (Nats-Expected-Last-Msg-Id or Nats-Msg-Id).
	ErrBatchPublishUnsupportedHeader jetstream.JetStreamError = &jsError{apiErr: &jetstream.APIError{ErrorCode: JSErrCodeBatchPublishUnsupportedHeader, Description: "batch publish unsupported header used (Nats-Expected-Last-Msg-Id or Nats-Msg-Id)", Code: 400}}

	// ErrBatchClosed is returned when attempting to use a batch that has been closed.
	ErrBatchClosed = &jsError{message: "batch publisher closed"}

	// ErrInvalidBatchAck is returned when JetStream ack from batch publish is
	// invalid.
	ErrInvalidBatchAck jetstream.JetStreamError = &jsError{message: "invalid jetstream batch publish response"}
)

type jsError struct {
	apiErr  *jetstream.APIError
	message string
}

func (err *jsError) APIError() *jetstream.APIError {
	return err.apiErr
}

func (err *jsError) Error() string {
	if err.apiErr != nil && err.apiErr.Description != "" {
		return err.apiErr.Error()
	}
	return fmt.Sprintf("nats: %s", err.message)
}

func (err *jsError) Unwrap() error {
	// Allow matching to embedded APIError in case there is one.
	if err.apiErr == nil {
		return nil
	}
	return err.apiErr
}
