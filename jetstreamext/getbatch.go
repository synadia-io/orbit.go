package jetstreamext

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/natsext"
)

type (
	// GetBatchOpt is a function that can be used to configure the behavior of
	// the GetBatch function.
	GetBatchOpt func(*getBatchOpts) error

	// GetLastForOpt is a function that can be used to configure the behavior of
	// the GetLastMessagesFor function.
	GetLastForOpt func(*getLastBatchOpts) error

	getBatchOpts struct {
		Seq       uint64     `json:"seq,omitempty"`
		NextFor   string     `json:"next_by_subj,omitempty"`
		Batch     int        `json:"batch,omitempty"`
		MaxBytes  int        `json:"max_bytes,omitempty"`
		StartTime *time.Time `json:"start_time,omitempty"`
	}

	getLastBatchOpts struct {
		MultiLastFor []string   `json:"multi_last,omitempty"`
		Batch        int        `json:"batch,omitempty"`
		UpToSeq      uint64     `json:"up_to_seq,omitempty"`
		UpToTime     *time.Time `json:"up_to_time,omitempty"`
	}
)

var (
	// ErrBatchUnsupported is returned when the server does not support batch
	// get (batch get is not supported by nats server >=2.11.0).
	ErrBatchUnsupported = errors.New("batch get not supported by server")

	// ErrInvalidResponse is returned when the response from the server is
	// invalid.
	ErrInvalidResponse = errors.New("invalid stream response")

	// ErrNoMessages is returned when there are no messages to fetch given the
	// provided options.
	ErrNoMessages = errors.New("no messages")

	// ErrInvalidOption is returned when an invalid option is provided.
	ErrInvalidOption = errors.New("invalid option")

	// ErrSubjectRequired is returned when no subjects are provided in GetLastMessagesFor.
	ErrSubjectRequired = errors.New("at least one subject is required")
)

// GetBatchSeq sets the sequence number from which to start fetching messages.
func GetBatchSeq(seq uint64) GetBatchOpt {
	return func(opts *getBatchOpts) error {
		if seq <= 0 {
			return fmt.Errorf("%w: sequence number has to be greater than 0", ErrInvalidOption)
		}
		if opts.StartTime != nil {
			return fmt.Errorf("%w: cannot set both start time and sequence number", ErrInvalidOption)
		}
		opts.Seq = seq
		return nil
	}
}

// GetBatchSubject sets the subject from which to start fetching messages.
// It may include wildcards.
func GetBatchSubject(subj string) GetBatchOpt {
	return func(opts *getBatchOpts) error {
		opts.NextFor = subj
		return nil
	}
}

// GetBatchMaxBytes sets the maximum number of bytes to fetch.
// The server will try to fetch messages until the maximum number of bytes is
// reached (or the batch size is reached).
func GetBatchMaxBytes(maxBytes int) GetBatchOpt {
	return func(opts *getBatchOpts) error {
		if maxBytes <= 0 {
			return fmt.Errorf("%w: max bytes has to be greater than 0", ErrInvalidOption)
		}
		opts.MaxBytes = maxBytes
		return nil
	}
}

// GetBatchStartTime sets the start time from which to fetch messages.
func GetBatchStartTime(startTime time.Time) GetBatchOpt {
	return func(opts *getBatchOpts) error {
		if opts.Seq != 0 {
			return fmt.Errorf("%w: cannot set both start time and sequence number", ErrInvalidOption)
		}
		opts.StartTime = &startTime
		return nil
	}
}

// GetBatch fetches a batch of messages from the specified stream.
// The batch size is determined by the `batch` parameter.
// The function returns an iterator that can be used to iterate over the
// messages.
// The iterator will return an error if there are no messages to fetch.
func GetBatch(ctx context.Context, js jetstream.JetStream, stream string, batch int, opts ...GetBatchOpt) (iter.Seq2[*jetstream.RawStreamMsg, error], error) {
	reqOpts := &getBatchOpts{
		Batch: batch,
	}

	for _, opt := range opts {
		if err := opt(reqOpts); err != nil {
			return nil, err
		}
	}
	if reqOpts.StartTime == nil && reqOpts.Seq == 0 {
		reqOpts.Seq = 1
	}

	reqJSON, err := json.Marshal(reqOpts)
	if err != nil {
		return nil, err
	}

	return getDirect(ctx, js, stream, reqJSON)
}

// GetLastMessagesUpToSeq sets the sequence number up to which to fetch messages
// (inclusive).
func GetLastMessagesUpToSeq(seq uint64) GetLastForOpt {
	return func(opts *getLastBatchOpts) error {
		if opts.UpToTime != nil {
			return fmt.Errorf("%w: cannot set both up to sequence and up to time", ErrInvalidOption)
		}
		opts.UpToSeq = seq
		return nil
	}
}

// GetLastMessagesUpToTime sets the time up to which to fetch messages.
func GetLastMessagesUpToTime(tm time.Time) GetLastForOpt {
	return func(opts *getLastBatchOpts) error {
		if opts.UpToSeq != 0 {
			return fmt.Errorf("%w: cannot set both up to sequence and up to time", ErrInvalidOption)
		}
		opts.UpToTime = &tm
		return nil
	}
}

// GetLastMessagesBatchSize sets the optional batch size for fetching messages
// from multiple subjects.
func GetLastMessagesBatchSize(batch int) GetLastForOpt {
	return func(opts *getLastBatchOpts) error {
		if batch <= 0 {
			return fmt.Errorf("%w: batch size has to be greater than 0", ErrInvalidOption)
		}
		opts.Batch = batch
		return nil
	}
}

// GetLastMessagesFor fetches the last messages for the specified subjects from
// the specified stream.
// The function returns an iterator that can be used to iterate over the
// messages.
// It can be configured to fetch messages up to a certain stream sequence number or
// time.
func GetLastMessagesFor(ctx context.Context, js jetstream.JetStream, stream string, subjects []string, opts ...GetLastForOpt) (iter.Seq2[*jetstream.RawStreamMsg, error], error) {
	if len(subjects) == 0 {
		return nil, ErrSubjectRequired
	}
	reqOpts := &getLastBatchOpts{
		MultiLastFor: subjects,
	}

	for _, opt := range opts {
		if err := opt(reqOpts); err != nil {
			return nil, err
		}
	}

	reqJSON, err := json.Marshal(reqOpts)
	if err != nil {
		return nil, err
	}

	return getDirect(ctx, js, stream, reqJSON)
}

func getDirect(ctx context.Context, js jetstream.JetStream, stream string, req []byte) (iter.Seq2[*jetstream.RawStreamMsg, error], error) {
	subj := getPrefixedSubject(js.Options(), fmt.Sprintf("DIRECT.GET.%s", stream))
	eobSentinel := func(msg *nats.Msg) bool {
		status, desc := msg.Header.Get(statusHdr), msg.Header.Get(descHdr)
		return len(msg.Data) == 0 && status == statusNoContent && desc == descEOB
	}
	resp, err := natsext.RequestMany(ctx, js.Conn(), subj, req, natsext.RequestManySentinel(eobSentinel))
	if err != nil {
		return nil, err
	}
	return func(yield func(*jetstream.RawStreamMsg, error) bool) {
		for msg, err := range resp {
			if err != nil {
				yield(nil, err)
				return
			}
			rawMsg, err := convertDirectGetMsgResponseToMsg(msg)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(rawMsg, nil) {
				break
			}
		}
	}, nil
}

var (
	statusHdr        = "Status"
	descHdr          = "Description"
	statusNoContent  = "204"
	statusNoMessages = "404"
	descEOB          = "EOB"
)

func convertDirectGetMsgResponseToMsg(msg *nats.Msg) (*jetstream.RawStreamMsg, error) {
	if len(msg.Data) == 0 {
		status := msg.Header.Get(statusHdr)
		if status == statusNoMessages {
			return nil, ErrNoMessages
		}
	}
	if len(msg.Header) == 0 {
		return nil, fmt.Errorf("%w: response should have headers", ErrInvalidResponse)
	}
	numPending := msg.Header.Get("Nats-Num-Pending")
	if numPending == "" {
		return nil, ErrBatchUnsupported
	}
	stream := msg.Header.Get(jetstream.StreamHeader)
	if stream == "" {
		return nil, fmt.Errorf("%w: missing stream header", ErrInvalidResponse)
	}

	seqStr := msg.Header.Get(jetstream.SequenceHeader)
	if seqStr == "" {
		return nil, fmt.Errorf("%w: missing sequence header", ErrInvalidResponse)
	}
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid sequence header '%s': %v", ErrInvalidResponse, seqStr, err)
	}
	timeStr := msg.Header.Get(jetstream.TimeStampHeaer)
	if timeStr == "" {
		return nil, fmt.Errorf("%w: missing timestamp header", ErrInvalidResponse)
	}

	tm, err := time.Parse(time.RFC3339Nano, timeStr)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid timestamp header '%s': %v", ErrInvalidResponse, timeStr, err)
	}
	subj := msg.Header.Get(jetstream.SubjectHeader)
	if subj == "" {
		return nil, fmt.Errorf("%w: missing subject header", ErrInvalidResponse)
	}
	return &jetstream.RawStreamMsg{
		Subject:  subj,
		Sequence: seq,
		Header:   msg.Header,
		Data:     msg.Data,
		Time:     tm,
	}, nil
}

func getPrefixedSubject(jsOpts jetstream.JetStreamOptions, subject string) string {
	var prefix string
	if jsOpts.APIPrefix != "" {
		if !strings.HasSuffix(jsOpts.APIPrefix, ".") {
			prefix = jsOpts.APIPrefix + "."
		} else {
			prefix = jsOpts.APIPrefix
		}
	} else if jsOpts.Domain != "" {
		prefix = fmt.Sprintf("$JS.%s.", jsOpts.Domain)
	} else {
		prefix = "$JS.API."
	}
	return fmt.Sprintf("%s%s", prefix, subject)
}
