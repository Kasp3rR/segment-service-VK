package postgres

import (
	"context"
	"crypto/sha256"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

func Migrate(ctx context.Context, pool *pgxpool.Pool, direction string) error {
	if direction != "up" && direction != "down" {
		return fmt.Errorf("migration direction must be up or down")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(73647301)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT name,checksum FROM schema_migrations ORDER BY name`)
	if err != nil {
		return err
	}
	applied := map[string]string{}
	for rows.Next() {
		var name, sum string
		if err = rows.Scan(&name, &sum); err != nil {
			rows.Close()
			return err
		}
		applied[name] = sum
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	names, err := fs.Glob(migrations, "migrations/*.up.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	known := map[string]bool{}
	for _, name := range names {
		data, e := migrations.ReadFile(name)
		if e != nil {
			return e
		}
		known[name] = true
		if checksum, ok := applied[name]; ok && checksum != fmt.Sprintf("%x", sha256.Sum256(data)) {
			return fmt.Errorf("migration checksum mismatch: %s", name)
		}
	}
	for name := range applied {
		if !known[name] {
			return fmt.Errorf("unknown applied migration: %s", name)
		}
	}
	if direction == "up" {
		for _, name := range names {
			if _, ok := applied[name]; ok {
				continue
			}
			data, _ := migrations.ReadFile(name)
			if _, err = tx.Exec(ctx, string(data)); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			if _, err = tx.Exec(ctx, `INSERT INTO schema_migrations(name,checksum) VALUES($1,$2)`, name, fmt.Sprintf("%x", sha256.Sum256(data))); err != nil {
				return err
			}
		}
	} else {
		for i := len(names) - 1; i >= 0; i-- {
			name := names[i]
			if _, ok := applied[name]; !ok {
				continue
			}
			data, e := migrations.ReadFile(strings.TrimSuffix(name, ".up.sql") + ".down.sql")
			if e != nil {
				return e
			}
			if _, err = tx.Exec(ctx, string(data)); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `DELETE FROM schema_migrations WHERE name=$1`, name); err != nil {
				return err
			}
			break
		}
	}
	return tx.Commit(ctx)
}
