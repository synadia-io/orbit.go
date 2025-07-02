package jetstreamext

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"strconv"
)

const (
	JSBatchId     = "Nats-Batch-Id"
	JSBatchSeq    = "Nats-Batch-Sequence"
	JSBatchCommit = "Nats-Batch-Commit"
)

var (
	ErrAtomicBatchEmpty = errors.New("atomic batch empty")
)

type (
	apiResponse struct {
		Type  string         `json:"type"`
		Error *nats.APIError `json:"error,omitempty"`
	}

	pubAckResponse struct {
		apiResponse
		*nats.PubAck
	}
)

// AtomicBatchPublish will send multiple messages as part of a single atomic batch.
func AtomicBatchPublish(ctx context.Context, nc *nats.Conn, msgs []*nats.Msg) (*nats.PubAck, error) {
	if ctx == nil {
		return nil, nats.ErrInvalidContext
	}
	if nc == nil {
		return nil, nats.ErrInvalidConnection
	}
	// Check whether the context is done already before making the request.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(msgs) == 0 {
		return nil, ErrAtomicBatchEmpty
	}

	id := nuid.Next()
	for i, msg := range msgs {
		msg.Header.Set(JSBatchId, id)
		msg.Header.Set(JSBatchSeq, strconv.Itoa(i+1))
		if i < len(msgs)-1 {
			// Ignore error here, the commit request will return the error.
			_ = nc.PublishMsg(msg)
			continue
		}

		msg.Header.Set(JSBatchCommit, "1")
		resp, err := nc.RequestMsgWithContext(ctx, msg)
		if err != nil && errors.Is(err, nats.ErrNoResponders) {
			return nil, jetstream.ErrNoStreamResponse
		}

		var ackResp pubAckResponse
		if err := json.Unmarshal(resp.Data, &ackResp); err != nil {
			return nil, jetstream.ErrInvalidJSAck
		}
		if ackResp.Error != nil {
			return nil, fmt.Errorf("nats: %w", ackResp.Error)
		}
		if ackResp.PubAck == nil || ackResp.PubAck.Stream == "" {
			return nil, jetstream.ErrInvalidJSAck
		}
		return ackResp.PubAck, nil
	}
	// Redundant check, this is already done on top.
	return nil, ErrAtomicBatchEmpty
}
