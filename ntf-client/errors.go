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

	// ErrServerNameAmbiguous is returned when the management service refuses a
	// stop, start, update or reload because more than one server holds the
	// name, with error code 023. It is returned wrapped together with
	// ErrService.
	ErrServerNameAmbiguous = errors.New("server name is held by more than one server")

	// ErrJetStreamNotReady is returned when a create given WithJetStreamWait
	// ends with JetStream not ready on every server, with error code 024: the
	// wait passed, every server refused the system credentials, or the
	// service closed. It is returned wrapped together with ErrService, and the
	// error text carries the service's reason, naming each server not ready.
	// The service leaves the instance in place, and the create returns it
	// alongside the error, when the service's answer names it, so the caller
	// can inspect or destroy it.
	ErrJetStreamNotReady = errors.New("JetStream did not become ready")

	// ErrInvalidCreateOption is returned, before any request is sent, when a
	// create option does not apply to the kind of instance being created,
	// WithServerNames or WithClusters is given empty, or WithClusters is given
	// together with cluster and server counts.
	ErrInvalidCreateOption = errors.New("invalid create option")

	// ErrTraceStore is returned when the TRACES object store cannot be opened,
	// listed, or read.
	ErrTraceStore = errors.New("could not access the TRACES object store")

	// ErrInvalidCapture is returned when a stored capture carries metadata that
	// cannot be parsed.
	ErrInvalidCapture = errors.New("invalid capture metadata")

	// ErrCaptureWait is returned when fewer captures than requested land before
	// the wait ends.
	ErrCaptureWait = errors.New("captures did not land in time")

	// ErrEvents is returned when an instance's events cannot be read, or an
	// event cannot be decoded.
	ErrEvents = errors.New("could not read instance events")

	// ErrNoEventStream is returned when past events are asked for but the
	// management server has no JetStream or no event stream.
	ErrNoEventStream = errors.New("events stream not found")
)
