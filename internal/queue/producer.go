package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

type BatchBalancer struct{}

func (BatchBalancer) Balance(message kafka.Message, partitions ...int) int {
	if len(partitions) == 0 {
		return 0
	}
	index := message.Partition % len(partitions)
	if index < 0 {
		index = 0
	}
	return partitions[index]
}

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     BatchBalancer{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    1,
		MaxAttempts:  3,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
}

func Send(ctx context.Context, writer *kafka.Writer, batch domain.Batch) error {
	raw, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	return writer.WriteMessages(ctx, kafka.Message{
		Key:       []byte(batch.JobID),
		Partition: batch.Number,
		Value:     raw,
	})
}

func (worker Worker) Publish(ctx context.Context, writer *kafka.Writer) {
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(ctx, worker.OperationTimeout)
		found, err := worker.Store.DispatchOne(operation, func(ctx context.Context, batch domain.Batch) error {
			publishing, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return Send(publishing, writer, batch)
		})
		cancel()
		if err != nil && ctx.Err() == nil {
			worker.Logger.Warn("outbox retry", zap.Error(err))
		}
		if !found || err != nil {
			if !Pause(ctx, worker.PollInterval) {
				return
			}
		}
	}
}
