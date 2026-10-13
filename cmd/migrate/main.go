package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/Kasp3rR/segment-service-VK/internal/config"
	"github.com/Kasp3rR/segment-service-VK/internal/postgres"
)

func main() {
	direction := flag.String("direction", "up", "up applies pending migrations; down reverts the last migration")
	flag.Parse()
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(ctx, cfg.DatabaseURL, cfg.MaxConnections)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Pool.Close()
	if err = postgres.Migrate(ctx, store.Pool, *direction); err != nil {
		log.Fatal(err)
	}
	log.Printf("migrations %s complete", *direction)
}
