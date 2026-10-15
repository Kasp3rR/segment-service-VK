package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kasp3rR/segment-service-VK/internal/postgres"
)

type Measurement struct {
	Name        string    `json:"name"`
	ExecutionMS []float64 `json:"execution_ms"`
	MedianMS    float64   `json:"median_ms"`
	RoundTripMS []float64 `json:"round_trip_ms"`
}

type Report struct {
	Date        time.Time         `json:"date"`
	PostgreSQL  string            `json:"postgresql"`
	Go          string            `json:"go"`
	Platform    string            `json:"platform"`
	Settings    map[string]string `json:"settings"`
	Users       int64             `json:"users"`
	Segments    int               `json:"segments"`
	Memberships int64             `json:"memberships"`
	UserID      int64             `json:"user_id"`
	Repeats     int               `json:"repeats"`
	Warmups     int               `json:"warmups"`
	Results     []Measurement     `json:"results"`
}

func main() {
	users := flag.Int64("users", 200000, "users in isolated benchmark schema")
	repeats := flag.Int("repeats", 20, "measured executions per variant")
	output := flag.String("output", "docs/sql-benchmark", "result directory")
	flag.Parse()
	if err := run(*users, *repeats, *output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(users int64, repeats int, output string) error {
	if users < 10 || users > 10000000 || repeats < 1 || repeats > 1000 {
		return fmt.Errorf("users must be 10..10000000; repeats must be 1..1000")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("TEST_DATABASE_URL is required")
	}
	if err := os.MkdirAll(output, 0755); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer admin.Close()
	schema := fmt.Sprintf("segment_benchmark_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup %s: %v\n", schema, err)
		}
	}()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}
	config.MaxConns = 1
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err = postgres.Migrate(ctx, pool, "up"); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "Preparing isolated benchmark data...")
	if _, err = pool.Exec(ctx, `INSERT INTO users(id) SELECT generate_series(1,$1::bigint)`, users); err != nil {
		return err
	}
	if _, err = pool.Exec(ctx, `INSERT INTO segments(code,name,description) SELECT 'BENCH_'||i,'Segment '||i,repeat('description ',10) FROM generate_series(1,100) i`); err != nil {
		return err
	}
	if _, err = pool.Exec(ctx, `INSERT INTO user_segments(user_id,segment_id) SELECT u.id,((u.id+k*13)%100)+1 FROM users u CROSS JOIN generate_series(0,9) k`); err != nil {
		return err
	}
	for _, table := range []string{"users", "segments", "user_segments"} {
		if _, err = pool.Exec(ctx, "VACUUM (ANALYZE) "+table); err != nil {
			return err
		}
	}
	report := Report{
		Date:        time.Now().UTC(),
		Go:          runtime.Version(),
		Platform:    runtime.GOOS + "/" + runtime.GOARCH,
		Users:       users,
		Segments:    100,
		Memberships: users * 10,
		UserID:      users/2 + 1,
		Repeats:     repeats,
		Warmups:     2,
		Settings:    map[string]string{},
	}
	if err = pool.QueryRow(ctx, "SELECT version()").Scan(&report.PostgreSQL); err != nil {
		return err
	}
	for _, name := range []string{"shared_buffers", "work_mem", "effective_cache_size", "max_parallel_workers_per_gather", "random_page_cost", "seq_page_cost", "jit"} {
		var value string
		if err = pool.QueryRow(ctx, "SHOW "+name).Scan(&value); err != nil {
			return err
		}
		report.Settings[name] = value
	}
	if err = os.WriteFile(filepath.Join(output, "query.sql"), []byte(postgres.UserSegmentsQuery+";\n"), 0644); err != nil {
		return err
	}
	if _, err = pool.Exec(ctx, `ALTER TABLE user_segments DROP CONSTRAINT user_segments_pkey`); err != nil {
		return err
	}
	for _, variant := range []string{"without_composite_index", "with_composite_index"} {
		if variant == "with_composite_index" {
			if _, err = pool.Exec(ctx, `ALTER TABLE user_segments ADD PRIMARY KEY(user_id,segment_id)`); err != nil {
				return err
			}
			if _, err = pool.Exec(ctx, `ANALYZE user_segments`); err != nil {
				return err
			}
		}
		measurement, err := measure(ctx, pool, variant, report.UserID, repeats, output)
		if err != nil {
			return err
		}
		report.Results = append(report.Results, measurement)
		fmt.Fprintf(os.Stderr, "%s median: %.3f ms\n", variant, measurement.MedianMS)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(output, "results.json"), append(data, '\n'), 0644)
}

func measure(ctx context.Context, pool *pgxpool.Pool, name string, userID int64, repeats int, output string) (Measurement, error) {
	result := Measurement{Name: name}
	for i := 0; i < repeats+2; i++ {
		started := time.Now()
		var raw []byte
		err := pool.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+postgres.UserSegmentsQuery, userID).Scan(&raw)
		if err != nil {
			return result, err
		}
		roundTrip := float64(time.Since(started).Microseconds()) / 1000
		var plan []struct {
			ExecutionTime float64 `json:"Execution Time"`
		}
		if err = json.Unmarshal(raw, &plan); err != nil {
			return result, err
		}
		if len(plan) != 1 {
			return result, fmt.Errorf("unexpected EXPLAIN result")
		}
		if i >= 2 {
			result.ExecutionMS = append(result.ExecutionMS, plan[0].ExecutionTime)
			result.RoundTripMS = append(result.RoundTripMS, roundTrip)
		}
		if i == 2 {
			if err = os.WriteFile(filepath.Join(output, name+".json"), raw, 0644); err != nil {
				return result, err
			}
		}
	}
	rows, err := pool.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS) "+postgres.UserSegmentsQuery, userID)
	if err != nil {
		return result, err
	}
	text := ""
	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			rows.Close()
			return result, err
		}
		text += line + "\n"
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	if err = os.WriteFile(filepath.Join(output, name+".txt"), []byte(text), 0644); err != nil {
		return result, err
	}
	sorted := append([]float64(nil), result.ExecutionMS...)
	sort.Float64s(sorted)
	result.MedianMS = sorted[len(sorted)/2]
	if len(sorted)%2 == 0 {
		result.MedianMS = (sorted[len(sorted)/2-1] + result.MedianMS) / 2
	}
	return result, nil
}
