package postgres

import (
	"context"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (store *Store) Quarantine(ctx context.Context, delivery domain.Delivery, payload []byte, cause error) error {
	tx, err := store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO kafka_dead_letters(topic,partition,message_offset,payload,reason) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, delivery.Topic, delivery.Partition, delivery.Offset, payload, shortError(cause)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
