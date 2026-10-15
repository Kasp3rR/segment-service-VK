package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/segmentio/kafka-go"

	"github.com/Kasp3rR/segment-service-VK/internal/distribution"
	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func Decode(raw []byte) (domain.Batch, error) {
	var batch domain.Batch
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&batch); err != nil {
		return batch, domain.ErrInvalidMessage
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return batch, domain.ErrInvalidMessage
	}
	return batch, distribution.ValidateBatch(batch)
}

func (worker Worker) Handle(ctx context.Context, message kafka.Message, consumerID string) error {
	delivery := domain.Delivery{
		Topic:      message.Topic,
		Partition:  message.Partition,
		Offset:     message.Offset,
		ConsumerID: consumerID,
	}
	batch, err := Decode(message.Value)
	if err == nil {
		_, err = worker.Store.ProcessBatch(ctx, batch, delivery)
	}
	if errors.Is(err, domain.ErrInvalidMessage) {
		return worker.Store.Quarantine(ctx, delivery, message.Value, err)
	}
	if errors.Is(err, domain.ErrBatchFailed) {
		return worker.Store.FailJob(ctx, batch, err)
	}
	if err != nil && distribution.ValidJobID(batch.JobID) {
		_ = worker.Store.RecordRetry(ctx, batch, err)
	}
	return err
}
