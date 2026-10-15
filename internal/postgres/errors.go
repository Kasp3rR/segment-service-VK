package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func mapError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		switch pgError.Code {
		case "23505":
			return domain.ErrConflict
		case "23503":
			return domain.ErrNotFound
		case "23514", "22001":
			return domain.ErrInvalid
		}
	}
	return err
}

func shortError(err error) string {
	message := []rune(err.Error())
	if len(message) > 2000 {
		message = message[:2000]
	}
	return string(message)
}
