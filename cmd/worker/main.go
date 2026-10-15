package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/config"
	"github.com/Kasp3rR/segment-service-VK/internal/postgres"
	"github.com/Kasp3rR/segment-service-VK/internal/queue"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()
	if err = run(logger); err != nil {
		logger.Error("worker stopped", zap.Error(err))
		os.Exit(1)
	}
}

func run(logger *zap.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := postgres.Open(startup, cfg.DatabaseURL, cfg.MaxConnections)
	if err != nil {
		return err
	}
	defer store.Pool.Close()
	if err = postgres.Migrate(startup, store.Pool, "up"); err != nil {
		return err
	}
	if err = queue.CheckTopic(startup, cfg.KafkaBrokers, cfg.KafkaTopic); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	instance, _ := os.Hostname()
	worker := queue.Worker{
		Store:            store,
		Brokers:          cfg.KafkaBrokers,
		Topic:            cfg.KafkaTopic,
		Group:            cfg.KafkaGroup,
		Instance:         instance,
		Logger:           logger,
		PollInterval:     cfg.OutboxPollInterval,
		OperationTimeout: cfg.WorkerTimeout,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.Run(ctx)
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		check, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := store.Pool.Ping(check); err != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		if err := queue.CheckTopic(check, cfg.KafkaBrokers, cfg.KafkaTopic); err != nil {
			http.Error(w, "kafka unavailable", 503)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ready\n"))
	})
	server := &http.Server{
		Addr:              cfg.WorkerHTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.ListenAndServe()
	}()
	logger.Info("worker started", zap.Int("consumers", 4), zap.String("topic", cfg.KafkaTopic), zap.String("group", cfg.KafkaGroup))
	select {
	case <-ctx.Done():
	case err = <-serverDone:
		stop()
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()
	if shutdownErr := server.Shutdown(shutdown); shutdownErr != nil {
		_ = server.Close()
	}
	select {
	case <-done:
	case <-shutdown.Done():
		return shutdown.Err()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
