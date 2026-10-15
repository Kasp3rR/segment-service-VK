package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func CheckTopic(ctx context.Context, brokers []string, topic string) error {
	if len(brokers) == 0 {
		return errors.New("no Kafka brokers configured")
	}
	dialer := kafka.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return err
	}
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return err
	}
	if len(partitions) < domain.Consumers {
		return fmt.Errorf("topic %s needs at least %d partitions; got %d", topic, domain.Consumers, len(partitions))
	}
	return nil
}
