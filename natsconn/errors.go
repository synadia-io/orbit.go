package natsconn

import "errors"

// NatsConnError represents an error type specific to the natsconn package.
// All errors returned by this package can be checked against the exported error variables.
type NatsConnError error

var (
	// ErrNilNatsConnConfig is returned when a nil configuration is provided to NewNatsConnection.
	ErrNilNatsConnConfig NatsConnError = errors.New("natsconn: nil config")

	// ErrFailToUseNatsContext is returned when the specified NATS context cannot be used.
	ErrFailToUseNatsContext NatsConnError = errors.New("natsconn: failed to use nats context")

	// ErrNoNatsServers is returned when no NATS servers are provided in the configuration
	// and no NATS context is specified.
	ErrNoNatsServers NatsConnError = errors.New("natsconn: no nats servers provided")

	// ErrFailedToExtractNkeySeed is returned when the NKey seed cannot be extracted
	// during NKey-based authentication.
	ErrFailedToExtractNkeySeed NatsConnError = errors.New("natsconn: failed to extract nkey seed")

	// ErrNatsConnectionFailed is returned when the connection to NATS servers fails.
	ErrNatsConnectionFailed NatsConnError = errors.New("natsconn: nats connection failed")
)

// errorWithContext wraps a NatsConnError with additional context from another error.
// This is used internally to provide more detailed error messages while preserving
// the ability to check error types.
func errorWithContext(nErr NatsConnError, err error) error {
	return errors.New(nErr.Error() + ": " + err.Error())
}
