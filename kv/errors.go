// Copyright 2025 The NATS Authors
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

package kv

import (
	"errors"
	"fmt"
)

type (
	// JetStreamError is an error result that happens when using JetStream.
	// In case of client-side error, [APIError] returns nil.
	JetStreamError interface {
		APIError() *APIError
		error
	}

	jsError struct {
		apiErr  *APIError
		message string
	}

	// APIError is included in all API responses if there was an error.
	APIError struct {
		Code        int       `json:"code"`
		ErrorCode   ErrorCode `json:"err_code"`
		Description string    `json:"description,omitempty"`
	}

	// ErrorCode represents error_code returned in response from JetStream API.
	ErrorCode uint16
)

const (
	JSErrCodeJetStreamNotEnabledForAccount ErrorCode = 10039
	JSErrCodeJetStreamNotEnabled           ErrorCode = 10076

	JSErrCodeStreamNotFound  ErrorCode = 10059
	JSErrCodeStreamNameInUse ErrorCode = 10058

	JSErrCodeConsumerCreate            ErrorCode = 10012
	JSErrCodeConsumerNotFound          ErrorCode = 10014
	JSErrCodeConsumerNameExists        ErrorCode = 10013
	JSErrCodeConsumerAlreadyExists     ErrorCode = 10105
	JSErrCodeConsumerExists            ErrorCode = 10148
	JSErrCodeDuplicateFilterSubjects   ErrorCode = 10136
	JSErrCodeOverlappingFilterSubjects ErrorCode = 10138
	JSErrCodeConsumerEmptyFilter       ErrorCode = 10139
	JSErrCodeConsumerDoesNotExist      ErrorCode = 10149

	JSErrCodeMessageNotFound ErrorCode = 10037

	JSErrCodeBadRequest ErrorCode = 10003

	JSErrCodeStreamWrongLastSequence ErrorCode = 10071
)

var (
	// ErrKeyExists is returned when attempting to create a key that already
	// exists.
	ErrKeyExists JetStreamError = &jsError{apiErr: &APIError{ErrorCode: JSErrCodeStreamWrongLastSequence, Code: 400}, message: "key exists"}

	// ErrKeyValueConfigRequired is returned when attempting to create a bucket
	// without a config.
	ErrKeyValueConfigRequired JetStreamError = &jsError{message: "config required"}

	// ErrInvalidBucketName is returned when attempting to create a bucket with
	// an invalid name.
	ErrInvalidBucketName JetStreamError = &jsError{message: "invalid bucket name"}

	// ErrInvalidKey is returned when attempting to create a key with an invalid
	// name.
	ErrInvalidKey JetStreamError = &jsError{message: "invalid key"}

	// ErrBucketExists is returned when attempting to create a bucket that
	// already exists and has a different configuration.
	ErrBucketExists JetStreamError = &jsError{message: "bucket name already in use"}

	// ErrBucketNotFound is returned when attempting to access a bucket that
	// does not exist.
	ErrBucketNotFound JetStreamError = &jsError{message: "bucket not found"}

	// ErrBadBucket is returned when attempting to access a bucket that is not a
	// key-value store.
	ErrBadBucket JetStreamError = &jsError{message: "bucket not valid key-value store"}

	// ErrKeyNotFound is returned when attempting to access a key that does not
	// exist.
	ErrKeyNotFound JetStreamError = &jsError{message: "key not found"}

	// ErrKeyDeleted is returned when attempting to access a key that was
	// deleted.
	ErrKeyDeleted JetStreamError = &jsError{message: "key was deleted"}

	// ErrHistoryToLarge is returned when provided history limit is larger than
	// 64.
	ErrHistoryTooLarge JetStreamError = &jsError{message: "history limited to a max of 64"}

	// ErrNoKeysFound is returned when no keys are found.
	ErrNoKeysFound JetStreamError = &jsError{message: "no keys found"}

	// ErrTTLOnDeleteNotSupported is returned when attempting to set a TTL
	// on a delete operation.
	ErrTTLOnDeleteNotSupported JetStreamError = &jsError{message: "TTL is not supported on delete"}

	// ErrLimitMarkerTTLNotSupported is returned when the connected jetstream API
	// does not support setting the LimitMarkerTTL.
	ErrLimitMarkerTTLNotSupported JetStreamError = &jsError{message: "limit marker TTLs not supported by server"}

	// ErrInvalidOption is returned when there is a collision between options.
	ErrInvalidOption JetStreamError = &jsError{message: "invalid option"}
)

// Error prints the JetStream API error code and description.
func (e *APIError) Error() string {
	return fmt.Sprintf("nats: API error: code=%d err_code=%d description=%s", e.Code, e.ErrorCode, e.Description)
}

// APIError implements the JetStreamError interface.
func (e *APIError) APIError() *APIError {
	return e
}

// Is matches against an APIError.
func (e *APIError) Is(err error) bool {
	if e == nil {
		return false
	}
	// Extract internal APIError to match against.
	var aerr *APIError
	ok := errors.As(err, &aerr)
	if !ok {
		return ok
	}
	return e.ErrorCode == aerr.ErrorCode
}

func (err *jsError) APIError() *APIError {
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
