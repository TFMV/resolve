package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/resolve/internal/config"
	"github.com/TFMV/resolve/internal/embed"
	"github.com/TFMV/resolve/internal/match"
	"github.com/TFMV/resolve/internal/weaviate"
)

func Run(cfg *config.Config) error {
	// Initialize the embedding service
	embeddingService := embed.NewHTTPClient(cfg)

	// Initialize Weaviate client
	weaviateClient, err := weaviate.NewClient(cfg, cfg.Embedding.EmbeddingDim)
	if err != nil {
		return fmt.Errorf("failed to initialize Weaviate client: %w", err)
	}

	// Initialize the match service
	matchService := match.NewService(cfg, weaviateClient, embeddingService)

	// Create server
	server := NewServer(cfg, weaviateClient, matchService, cfg.Embedding.EmbeddingDim)
	server.registerRoutes()

	// Configure HTTP server
	addr := fmt.Sprintf("%s:%d", cfg.API.Host, cfg.API.Port)
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      server.router,
		ReadTimeout:  time.Duration(cfg.API.ReadTimeoutSecs) * time.Second,
		WriteTimeout: time.Duration(cfg.API.WriteTimeoutSecs) * time.Second,
		IdleTimeout:  time.Duration(cfg.API.IdleTimeoutSecs) * time.Second,
	}

	// Set server reference
	server.httpServer = httpServer

	// Start server in a goroutine
	go func() {
		log.Printf("Starting server on %s", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Create context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Attempt graceful shutdown
	if err := httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("server forced to shutdown: %w", err)
	}

	log.Println("Server exited gracefully")
	return nil
}
