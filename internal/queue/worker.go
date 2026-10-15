package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

type Store interface {
	DispatchOne(context.Context, func(context.Context, domain.Batch) error) (bool, error)
	ProcessBatch(context.Context, domain.Batch, domain.Delivery) (bool, error)
	RecordRetry(context.Context, domain.Batch, error) error
	FailJob(context.Context, domain.Batch, error) error
	Quarantine(context.Context, domain.Delivery, []byte, error) error
}

type Worker struct {
	Store            Store
	Brokers          []string
	Topic            string
	Group            string
	Instance         string
	Logger           *zap.Logger
	PollInterval     time.Duration
	OperationTimeout time.Duration
}

func (worker Worker) Run(ctx context.Context) {
	writer := NewWriter(worker.Brokers, worker.Topic)
	defer writer.Close()
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		worker.Publish(ctx, writer)
	}()
	for i := 0; i < domain.Consumers; i++ {
		id := fmt.Sprintf("%s/%d", worker.Instance, i)
		group.Add(1)
		go func() {
			defer group.Done()
			worker.Consume(ctx, worker.NewReader(), id)
		}()
	}
	group.Wait()
}
