package queue

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Reader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

func (worker Worker) NewReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:           worker.Brokers,
		Topic:             worker.Topic,
		GroupID:           worker.Group,
		MinBytes:          1,
		MaxBytes:          10 << 20,
		CommitInterval:    0,
		StartOffset:       kafka.FirstOffset,
		GroupBalancers:    []kafka.GroupBalancer{kafka.RoundRobinGroupBalancer{}},
		SessionTimeout:    10 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		MaxWait:           500 * time.Millisecond,
	})
}

func (worker Worker) Consume(ctx context.Context, reader Reader, consumerID string) {
	defer reader.Close()
	for ctx.Err() == nil {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			worker.Logger.Warn("kafka fetch failed", zap.String("consumer", consumerID), zap.Error(err))
			if !Pause(ctx, time.Second) {
				return
			}
			continue
		}
		for attempt := 0; ctx.Err() == nil; attempt++ {
			operation, cancel := context.WithTimeout(ctx, worker.OperationTimeout)
			err = worker.Handle(operation, message, consumerID)
			if err == nil {
				err = reader.CommitMessages(operation, message)
			}
			cancel()
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}
			worker.Logger.Warn("kafka message retry", zap.String("consumer", consumerID), zap.Int("partition", message.Partition), zap.Int64("offset", message.Offset), zap.Error(err))
			if !Pause(ctx, Backoff(attempt)) {
				return
			}
		}
	}
}
