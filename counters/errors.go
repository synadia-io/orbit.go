package counters

import "errors"

var (
	// ErrCounterNotEnabled is returned when trying to create a Counter
	// from a stream that doesn't have AllowMsgCounter enabled.
	ErrCounterNotEnabled = errors.New("stream is not configured for counters (AllowMsgCounter must be true)")

	// ErrDirectAccessRequired is returned when trying to create a Counter
	// from a stream that doesn't have AllowDirect enabled.
	ErrDirectAccessRequired = errors.New("stream must be configured for direct access (AllowDirect must be true)")

	// ErrInvalidCounterValue is returned when a counter value is invalid
	ErrInvalidCounterValue = errors.New("invalid counter value")

	// ErrCounterNotFound is returned when a counter subject doesn't exist.
	ErrCounterNotFound = errors.New("counter not found")

	// ErrNoCounterForSubject is returned when trying to access a counter
	// for a subject that has not been initialized.
	ErrNoCounterForSubject = errors.New("counter not initialized for subject")
)
