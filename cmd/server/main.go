package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ling-qian/Go-Live-Room/internal/cache"
	"github.com/ling-qian/Go-Live-Room/internal/room"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	store := room.NewStore()
	var redisClient *cache.Client
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		redisClient = cache.New(addr, os.Getenv("REDIS_PASSWORD"), 0)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := redisClient.Ping(ctx); err != nil {
			logger.Warn("redis unavailable; continuing without presence cache", "error", err)
			redisClient = nil
		}
		cancel()
	}
	server := &http.Server{
		Addr:              envOr("ADDR", ":8080"),
		Handler:           room.NewHandler(store, logger, redisClient),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("server starting", "addr", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
			os.Exit(1)
		}
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
