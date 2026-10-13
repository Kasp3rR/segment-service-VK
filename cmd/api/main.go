package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/config"
	"github.com/Kasp3rR/segment-service-VK/internal/httpapi"
	"github.com/Kasp3rR/segment-service-VK/internal/postgres"
	"github.com/Kasp3rR/segment-service-VK/internal/service"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()
	gin.SetMode(gin.ReleaseMode)
	if err := run(logger); err != nil {
		logger.Error("service stopped", zap.Error(err))
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: httpapi.New(service.New(store), store.Pool.Ping, logger, cfg.RequestTimeout), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: cfg.RequestTimeout + 5*time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() {
		logger.Info("HTTP listening", zap.String("address", cfg.HTTPAddr))
		done <- server.ListenAndServe()
	}()
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()
	if err = server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		return err
	}
	err = <-done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
