package ntf

import "errors"

var (
	// ErrConnect is returned when the management service cannot be reached.
	ErrConnect = errors.New("could not connect to the management service")

	// ErrRequest is returned when a request cannot be encoded or does not reach
	// the management service, or no reply arrives in time.
	ErrRequest = errors.New("request to the management service failed")

	// ErrService is returned when the management service answers a request with
	// an error. The service's message follows in the error text.
	ErrService = errors.New("management service returned an error")

	// ErrInvalidResponse is returned when a reply from the management service
	// cannot be decoded.
	ErrInvalidResponse = errors.New("invalid response from the management service")

	// ErrServerRequired is returned when a per-server operation is called
	// without a server name.
	ErrServerRequired = errors.New("server is required")

	// ErrInstanceNotFound is returned when the management service holds no
	// instance with the requested ID.
	ErrInstanceNotFound = errors.New("instance not found")

	// ErrTraceStore is returned when the TRACES object store cannot be opened,
	// listed, or read.
	ErrTraceStore = errors.New("could not access the TRACES object store")

	// ErrInvalidCapture is returned when a stored capture carries metadata that
	// cannot be parsed.
	ErrInvalidCapture = errors.New("invalid capture metadata")

	// ErrCaptureWait is returned when fewer captures than requested land before
	// the wait ends.
	ErrCaptureWait = errors.New("captures did not land in time")
)
